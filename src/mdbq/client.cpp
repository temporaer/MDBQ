#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <mongo/client/dbclient.h>
#include "client.hpp"

namespace mdbq
{
    struct ClientImpl{
        mongo::DBClientConnection m_con;
        mongo::BSONObj m_current_task;
        unsigned int m_interval;
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

        mongo::BSONObj res, cmd;
        cmd = BSON(
                "findAndModify" << "gtest_jobs"<<
                "query" << BSON("$and" <<
                    BSON_ARRAY(
                        BSON("finished" <<mongo::LT<< 0) <<
                        BSON("started" << mongo::LT<< 0)))<<
                "update"<<BSON("$set"<<
                    BSON("started"<<stime)));
        std::cout << "cmd: "<< cmd<<std::endl;
        //m_ptr->m_con.runCommand(m_prefix+"_jobs",cmd, res);
        m_ptr->m_con.runCommand("test",cmd, res);
        std::cout << "res: "<<res<<std::endl;
        if(res["value"].isNull())
            return false;

        ct = res["value"].Obj();
        o = ct["payload"].Obj();
        return true;
    }
    void Client::finish(const mongo::BSONObj& result){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you finish!");
        }

        long long int ftime = time(NULL);
        m_ptr->m_con.update(m_prefix+"_jobs",
                QUERY("_id"<<ct["_id"]),
                BSON("$set"<<BSON(
                    "finished"<<ftime<<
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

}
