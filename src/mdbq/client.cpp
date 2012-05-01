#include <boost/asio.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>
#include <mongo/client/dbclient.h>
#include <mongo/client/gridfs.h>
#include "client.hpp"
#include "common.hpp"

namespace mdbq
{
    struct ClientImpl{
        mongo::DBClientConnection m_con;
        mongo::BSONObj            m_current_task;
        std::auto_ptr<mongo::GridFS>             m_fs;
        long long int             m_current_task_stime;
        long long int             m_running_nr;
        //std::auto_ptr<mongo::BSONArrayBuilder>   m_log;
        std::vector<mongo::BSONObj> m_log;
        unsigned int              m_interval;
        std::auto_ptr<boost::asio::deadline_timer> m_timer;
        void update_check(Client* c, const boost::system::error_code& error){
            mongo::BSONObj task;
            if(c->get_next_task(task))
                c->handle_task(task);
            if(!error){
                m_timer->expires_at(m_timer->expires_at() + boost::posix_time::seconds(m_interval));
                m_timer->async_wait(boost::bind(&ClientImpl::update_check,this,c,boost::asio::placeholders::error));
            }
        }
    };
    Client::Client(const std::string& url, const std::string& prefix)
        : m_jobcol(prefix+".jobs")
        , m_logcol(prefix+".log")
    {
        m_ptr.reset(new ClientImpl());
        m_ptr->m_con.connect(url);

        std::string db, col;
        std::string::size_type pos = prefix.find( "." );
        if ( pos != std::string::npos ){
            db  = prefix.substr( 0, pos );
            col = prefix.substr( pos + 1 );
            m_db = db;
        }else{
            throw std::runtime_error("cannot parse prefix `"+prefix+"'");
        }
        m_fscol = col + "_fs";// Note: may not contain any "."!!!
        //m_fscol = db+".fs";
        m_ptr->m_fs.reset(new mongo::GridFS(m_ptr->m_con, db, m_fscol));
    }
    bool Client::get_next_task(mongo::BSONObj& o){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(!ct.isEmpty()){
            throw std::runtime_error("do tasks one by one, please!");
        }
        long long int stime = time(NULL);

        std::string col;
        std::string::size_type pos = m_jobcol.find( "." );
        if ( pos != std::string::npos )
            col = m_jobcol.substr( pos + 1 );

        mongo::BSONObj res, cmd;
        cmd = BSON(
                "findAndModify" << col<<
                "query" << BSON("state" << TS_OPEN)<<
                "update"<<BSON("$set"<<
                    BSON("stime"<<stime
                        <<"state"<<TS_ASSIGNED)));
        //std::cout << "cmd: "<< cmd<<std::endl;
        //m_ptr->m_con.runCommand(m_jobcol,cmd, res);
        m_ptr->m_con.runCommand(m_db,cmd, res);
        //std::cout << "res: "<<res<<std::endl;
        if(!res["value"].isABSONObj())
            return false;

        ct = res["value"].Obj().copy();
        m_ptr->m_current_task_stime = stime;
        m_ptr->m_running_nr = 0;
        o = ct["payload"].Obj();

        // start logging
        m_ptr->m_log.clear();
        return true;
    }
    void Client::finish(const mongo::BSONObj& result, bool ok){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you finish!");
        }

        this->checkpoint(); // flush logs

        long long int ftime = time(NULL);
        m_ptr->m_con.update(m_jobcol,
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
        std::cerr <<"WARNING: got a task, but not handling it!"<<std::endl;
        finish(BSON("error"<<true));
    }
    Client::~Client(){}
    void Client::log(const mongo::BSONObj& msg){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you log something about it!");
        }
        m_ptr->m_log.push_back(BSON( 
                    mongo::GENOID<<
                    "taskid"<<ct["_id"]<<
                    "nr" << m_ptr->m_running_nr++ <<
                    "ltime"<<(long long int)time(NULL)<<
                    "msg"<<msg));
    }
    void Client::log(const char* ptr, size_t len, const mongo::BSONObj& msg){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you log something about it!");
        }

        boost::uuids::basic_random_generator<boost::mt19937> gen;
        mongo::BSONObj ret = m_ptr->m_fs->storeFile(ptr,len, boost::lexical_cast<std::string>(gen()));
        {
            mongo::BSONObjBuilder bob;
            bob.appendElements(ret);
            bob.appendElements(msg);
            m_ptr->m_con.update(m_fscol+".files",
                    BSON("filename"<<ret.getField("filename")),
                    bob.obj(),false,false);
            std::string err = m_ptr->m_con.getLastError();
            if(err.size())
                throw std::runtime_error(err);
        }

        mongo::BSONObjBuilder bob;
        bob.appendElements(msg);
        bob.append("filename",ret["filename"].String());
        m_ptr->m_log.push_back(BSON(
                    mongo::GENOID<<
                    "taskid"<<ct["_id"]<<
                    "nr" << m_ptr->m_running_nr++ <<
                    "ltime"<<(long long int)time(NULL)<<
                    "filename" << ret["filename"].String()<<
                    "msg"<<msg
                    ));
    }
    void Client::checkpoint(){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("get a task first before you call checkpoints!");
        }

        {   // first, check for a timeout
            mongo::BSONObj failcheck = m_ptr->m_con.findOne(m_jobcol, 
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
        m_ptr->m_con.update(m_jobcol,
                QUERY("_id"<<ct["_id"]),
                BSON( "$set"<<BSON("ping"<<ctime)));

        std::string err = m_ptr->m_con.getLastError();
        if(err.size())
            throw std::runtime_error(err);

        if(m_ptr->m_log.size()) {
            m_ptr->m_con.insert(m_logcol, m_ptr->m_log);
            m_ptr->m_log.clear();
            std::string err = m_ptr->m_con.getLastError();
            if(err.size())
                throw std::runtime_error(err);
        }

    }
    std::vector<mongo::BSONObj> 
    Client::get_log(const mongo::BSONObj& task){
        std::auto_ptr<mongo::DBClientCursor> p =
            m_ptr->m_con.query( m_logcol, 
                    QUERY("taskid" << task["_id"]).sort("nr"));
        std::vector<mongo::BSONObj> log;
        while(p->more()){
            mongo::BSONObj f = p->next();
            log.push_back(f);
        }
        return log;
    }

}
