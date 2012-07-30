#include <boost/asio.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>
#include <mongo/client/dbclient.h>
#include <mongo/client/gridfs.h>
#include "client.hpp"
#include "common.hpp"
#include "date_time.hpp"

#ifdef NDEBUG
#  define CHECK_DB_ERR(CON)
#else
#  define CHECK_DB_ERR(CON)\
            {\
                string e = (CON).getLastError();\
                if(!e.empty()){\
                    throw std::runtime_error("MDBQC: error_code!=0, failing: " + e + "\n" + (CON).getLastErrorDetailed().toString() );\
                }\
            }
#endif

namespace mdbq
{

    struct ClientImpl{
        mongo::DBClientConnection m_con;
        mongo::BSONObj            m_current_task;
        std::auto_ptr<mongo::GridFS>             m_fs;
        boost::posix_time::ptime  m_current_task_book_time;
        boost::posix_time::ptime  m_current_task_timeout_time;
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
        CHECK_DB_ERR(m_ptr->m_con);

        std::string db, col;
        std::string::size_type pos = prefix.find( "." );
        if ( pos != std::string::npos ){
            db  = prefix.substr( 0, pos );
            col = prefix.substr( pos + 1 );
            m_db = db;
        }else{
            throw std::runtime_error("MDBQC: cannot parse prefix `"+prefix+"'");
        }
        m_fscol = col + "_fs";// Note: may not contain any "."!!!
        //m_fscol = db+".fs";
        m_ptr->m_fs.reset(new mongo::GridFS(m_ptr->m_con, db, m_fscol));
    }
    bool Client::get_next_task(mongo::BSONObj& o){
        if(!m_ptr->m_current_task.isEmpty()){
            throw std::runtime_error("MDBQC: do tasks one by one, please!");
        }
        boost::posix_time::ptime now = universal_date_time();
        mongo::BSONArray bson_now(ptime_to_bson(now));

        std::string col;
        std::string::size_type pos = m_jobcol.find( "." );
        if ( pos != std::string::npos )
            col = m_jobcol.substr( pos + 1 );

        std::string hostname(256, '\0');
        gethostname(&hostname[0], 256);
        unsigned int pid = getpid();

        mongo::BSONObj res, cmd;
        cmd = BSON(
                "findAndModify" << col<<
                "query" << BSON("state" << TS_NEW)<<
                "update"<<BSON("$set"<<
                    BSON("book_time"<<bson_now
                        <<"state"<<TS_RUNNING
                        <<"refresh_time"<<bson_now
                        <<"owner"<<BSON_ARRAY(&hostname[0]<<pid))));
        //std::cout << "cmd: "<< cmd<<std::endl;
        //m_ptr->m_con.runCommand(m_jobcol,cmd, res);
        m_ptr->m_con.runCommand(m_db,cmd, res);
        CHECK_DB_ERR(m_ptr->m_con);
        //std::cout << "res: "<<res<<std::endl;
        if(!res["value"].isABSONObj())
            return false;

        m_ptr->m_current_task = res["value"].Obj().copy();
        m_ptr->m_current_task_book_time = now;
        m_ptr->m_running_nr = 0;
        m_ptr->m_current_task_timeout_time = now + boost::posix_time::seconds(m_ptr->m_current_task["timeout"].Int());
        o = m_ptr->m_current_task["spec"].Obj();

        // start logging
        m_ptr->m_log.clear();
        return true;
    }
    void Client::finish(const mongo::BSONObj& result, bool ok){
        const mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you finish!");
        }

        this->checkpoint(false); // flush logs, do not check for timeout

        boost::posix_time::ptime finish_time = universal_date_time();
        if(ok)
            m_ptr->m_con.update(m_jobcol,
                    QUERY("_id"<<ct["_id"]),
                    BSON("$set"<<BSON(
                        "state"<<TS_OK<<
                        "finish_time"<<ptime_to_bson(finish_time)<<
                        "result"<<result)));
        else
            m_ptr->m_con.update(m_jobcol,
                    QUERY("_id"<<ct["_id"]),
                    BSON("$set"<<BSON(
                        "state"<<TS_FAILED<<
                        "failure_time"<<ptime_to_bson(finish_time)<<
                        "error"<<result)));
        CHECK_DB_ERR(m_ptr->m_con);
        m_ptr->m_current_task = mongo::BSONObj(); // empty, call get_next_task.
    }
    void Client::reg(boost::asio::io_service& io_service, unsigned int interval){
        m_ptr->m_interval = interval;
        m_ptr->m_timer.reset(new boost::asio::deadline_timer(io_service, boost::posix_time::seconds(interval)));
        m_ptr->m_timer->async_wait(boost::bind(&ClientImpl::update_check, m_ptr.get(), this, boost::asio::placeholders::error));
    }
    
    void Client::handle_task(const mongo::BSONObj& o){
        std::cerr <<"MDBQC: WARNING: got a task, but no handler defined!"<<std::endl;
        finish(BSON("error"<<true));
    }
    Client::~Client(){ }
    void Client::log(const mongo::BSONObj& msg){
        const mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you log something about it!");
        }
        boost::posix_time::ptime now = universal_date_time();
        m_ptr->m_log.push_back(BSON( 
                    mongo::GENOID<<
                    "taskid"<<ct["_id"]<<
                    "nr" << m_ptr->m_running_nr++ <<
                    "timestamp"<< ptime_to_bson(now)<<
                    "msg"<<msg));
    }
    void Client::log(const char* ptr, size_t len, const mongo::BSONObj& msg){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you log something about it!");
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
            CHECK_DB_ERR(m_ptr->m_con);
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
    void Client::checkpoint(bool check_for_timeout){
        const mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you call checkpoints!");
        }

        if(check_for_timeout){   // first, check whether the task has timed out.
            boost::posix_time::ptime now = universal_date_time();
            if(now >= m_ptr->m_current_task_timeout_time){
                std::string hostname(256, '\0');
                gethostname(&hostname[0], 256);
                unsigned int pid = getpid();

                // set to failed in DB
                m_ptr->m_con.update(m_jobcol, 
                        QUERY("_id"<<ct["_id"] << 
                            // do not overwrite job that has been taken by someone else!
                            // this may happen due to timeouts and rescheduling.
                            "owner"<<BSON_ARRAY(&hostname[0] << pid)
                            ),
                        BSON("$set" << 
                            BSON("state"<<TS_FAILED<< 
                                 "error"<<"timeout")));
                CHECK_DB_ERR(m_ptr->m_con);

                // clean up current state
                m_ptr->m_current_task = mongo::BSONObj();
                m_ptr->m_current_task_book_time = boost::posix_time::pos_infin;
                m_ptr->m_current_task_timeout_time = boost::posix_time::pos_infin;

                throw timeout_exception();
            }
        }

        boost::posix_time::ptime now = universal_date_time();
        m_ptr->m_con.update(m_jobcol,
                QUERY("_id"<<ct["_id"]),
                BSON( "$set"<<BSON("refresh_time"<<ptime_to_bson(now))));
        CHECK_DB_ERR(m_ptr->m_con);

        if(m_ptr->m_log.size()) {
            m_ptr->m_con.insert(m_logcol, m_ptr->m_log);
            m_ptr->m_log.clear();
            CHECK_DB_ERR(m_ptr->m_con);
        }

    }
    std::vector<mongo::BSONObj> 
    Client::get_log(const mongo::BSONObj& task){
        std::auto_ptr<mongo::DBClientCursor> p =
            m_ptr->m_con.query( m_logcol, 
                    QUERY("taskid" << task["_id"]).sort("nr"));
        CHECK_DB_ERR(m_ptr->m_con);
        std::vector<mongo::BSONObj> log;
        while(p->more()){
            mongo::BSONObj f = p->next();
            log.push_back(f);
        }
        return log;
    }

}
