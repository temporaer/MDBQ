#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <mongo/client/dbclient.h>
#include "client.hpp"
#include "common.hpp"

namespace mdbq
{
    struct ClientImpl{
        mongo::DBClientConnection m_con;
        mongo::BSONObj            m_current_task;
        long long int             m_current_task_stime;
        std::auto_ptr<mongo::BSONArrayBuilder>   m_log;
        unsigned int              m_interval;
        std::auto_ptr<boost::asio::deadline_timer> m_timer;
        void update_check(Client* c, const boost::system::error_code& error){
            mongo::BSONObj task;
            if(c->get_next_task(task))
                c->handle_task(task);
            if(!error){
                //Reschdule ourself
                m_timer->expires_at(m_timer->expires_at() + boost::posix_time::seconds(m_interval));
                m_timer->async_wait(boost::bind(&ClientImpl::update_check,this,c,boost::asio::placeholders::error));
            }
        }
    };
    Client::Client(const std::string& url, const std::string& prefix)
        : m_prefix(prefix)
    {
        m_ptr.reset(new ClientImpl());
        m_ptr->m_con.connect(url);
    }
    bool Client::get_next_task(mongo::BSONObj& o){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(!ct.isEmpty()){
            throw std::runtime_error("do tasks one by one, please!");
        }
        long long int stime = time(NULL);

        std::string col;
        std::string::size_type pos = m_prefix.find( "." );
        if ( pos != std::string::npos )
            col = m_prefix.substr( pos + 1 );

        mongo::BSONObj res, cmd;
        cmd = BSON(
                "findAndModify" << col+"_jobs"<<
                "query" << BSON("state" << TS_OPEN)<<
                "update"<<BSON("$set"<<
                    BSON("stime"<<stime
                        <<"state"<<TS_ASSIGNED)));
        std::cout << "cmd: "<< cmd<<std::endl;
        //m_ptr->m_con.runCommand(m_prefix+"_jobs",cmd, res);
        m_ptr->m_con.runCommand("test",cmd, res);
        std::cout << "res: "<<res<<std::endl;
        if(res["value"].isNull())
            return false;

        ct = res["value"].Obj().copy();
        m_ptr->m_current_task_stime = stime;
        o = ct["payload"].Obj();

        // start logging
        m_ptr->m_log.reset(new mongo::BSONArrayBuilder());
        return true;
    }
    void Client::finish(const mongo::BSONObj& result, bool ok){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you finish!");
        }

        this->checkpoint(); // flush logs

        long long int ftime = time(NULL);
        m_ptr->m_con.update(m_prefix+"_jobs",
                QUERY("_id"<<ct["_id"]),
                BSON("$set"<<BSON(
                    "state"<<(ok?TS_OK:TS_FAILED)<<
                    "ftime"<<ftime<<
                    "result"<<result)));
        ct = mongo::BSONObj(); // empty, call get_next_task.
    }
    void Client::reg(boost::asio::io_service& io_service, unsigned int interval){
        m_ptr->m_interval = interval;
        m_ptr->m_timer.reset(new boost::asio::deadline_timer(io_service, boost::posix_time::seconds(interval)));
        m_ptr->m_timer->async_wait(boost::bind(&ClientImpl::update_check, m_ptr.get(), this, boost::asio::placeholders::error));
    }
    
    void Client::handle_task(const mongo::BSONObj& o){
        std::cout <<"WARNING: got a task, but not handling it!"<<std::endl;
        finish(BSON("error"<<true));
    }
    Client::~Client(){}
    void Client::log(const mongo::BSONObj& msg){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you log something about it!");
        }
        m_ptr->m_log->append(msg);
    }
    void Client::checkpoint(){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you call checkpoints!");
        }

        {   // first, check for a timeout
            mongo::BSONObj failcheck = m_ptr->m_con.findOne(m_prefix+"_jobs", 
                    QUERY(
                        "_id"<<ct["_id"]<<
                        "$or"<<BSON_ARRAY(
                            BSON("state"<<TS_FAILED) <<
                            BSON("stime"<<mongo::NE<<m_ptr->m_current_task_stime))),
                    &BSON("_id"<<1));
            if(!failcheck.isEmpty()){
                // clean up current state
                m_ptr->m_current_task = mongo::BSONObj();
                m_ptr->m_current_task_stime = 0;
                throw timeout_exception();
            }
        }

        long long int ctime = time(NULL);
        mongo::BSONObj update =                 
            BSON(
                    "$set"<<BSON(
                            "state"<<TS_ASSIGNED<<
                            "ping"<<ctime) <<
                    "$pushAll"<< BSON(
                            "log"<<m_ptr->m_log->arr())); 
        std::cout << "Update: "<<update<<std::endl;
        m_ptr->m_con.update(m_prefix+"_jobs",
                QUERY("_id"<<ct["_id"]),
                update);
        m_ptr->m_log.reset(new mongo::BSONArrayBuilder());


        std::string err = m_ptr->m_con.getLastError();
        if(err.size())
            throw std::runtime_error(err);
    }

}
