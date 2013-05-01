#include <boost/format.hpp>
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
                std::string e = (CON).getLastError();\
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
        mongo::BSONObj            m_task_selector;
        std::auto_ptr<mongo::GridFS>             m_fs;
        boost::posix_time::ptime  m_current_task_timeout_time;
        long long int             m_running_nr;
        //std::auto_ptr<mongo::BSONArrayBuilder>   m_log;
        std::vector<mongo::BSONObj> m_log;
        float              m_interval;
        std::auto_ptr<boost::asio::deadline_timer> m_timer;
        void update_check(Client* c, const boost::system::error_code& error){
            mongo::BSONObj task;
            if(c->get_next_task(task))
                c->handle_task(task);
            if(!error){
                unsigned int ms;
                if(m_interval <= 1.f)
                    ms = 1000*(m_interval/2 + drand48() * (m_interval/2));
                else
                    ms = 1000*(1 + drand48() * (m_interval-1));
                m_timer->expires_at(m_timer->expires_at() + boost::posix_time::millisec(ms));
                m_timer->async_wait(boost::bind(&ClientImpl::update_check,this,c,boost::asio::placeholders::error));
            }
        }
    };
    Client::Client(const std::string& url, const std::string& prefix)
        : m_jobcol(prefix+".jobs")
        , m_logcol(prefix+".log")
        , m_fscol(prefix+".fs")
        , m_verbose(false)
    {
        m_ptr.reset(new ClientImpl());
        m_ptr->m_con.connect(url);
        CHECK_DB_ERR(m_ptr->m_con);

        m_db = prefix;
        m_ptr->m_fs.reset(new mongo::GridFS(m_ptr->m_con, m_db, "fs"));
    }
    Client::Client(const std::string& url, const std::string& prefix, const mongo::BSONObj& query)
        : m_jobcol(prefix+".jobs")
        , m_logcol(prefix+".log")
        , m_fscol(prefix+".fs")
        , m_verbose(false)
    {
        m_ptr.reset(new ClientImpl());
        m_ptr->m_con.connect(url);
        m_ptr->m_task_selector = query;
        CHECK_DB_ERR(m_ptr->m_con);

        m_db = prefix;
        m_ptr->m_fs.reset(new mongo::GridFS(m_ptr->m_con, m_db, "fs"));
    }
    bool Client::get_next_task(mongo::BSONObj& o){
        if(!m_ptr->m_current_task.isEmpty()){
            throw std::runtime_error("MDBQC: do tasks one by one, please!");
        }
        boost::posix_time::ptime now = universal_date_time();

        std::string hostname(256, '\0');
        gethostname(&hostname[0], 256);
        std::string hostname_pid = (boost::format("%s:%d") % &hostname[0] % getpid()).str();

        mongo::BSONObjBuilder queryb;
        mongo::BSONObj res, cmd, query;
        queryb.append("state", TS_NEW);
        if(! m_ptr->m_task_selector.isEmpty())
            queryb.appendElements(m_ptr->m_task_selector);
        query = queryb.obj();
        cmd = BSON(
                "findAndModify" << "jobs" <<
                "query" << query <<
                "update"<<BSON("$set"<<
                    BSON("book_time"<<to_mongo_date(now)
                        <<"state"<<TS_RUNNING
                        <<"result.status"<<"running"
                        <<"refresh_time"<<to_mongo_date(now)
                        <<"owner"<<hostname_pid)));
        //std::cout << "cmd: "<< cmd<<std::endl;
        //m_ptr->m_con.runCommand(m_jobcol,cmd, res);
        m_ptr->m_con.runCommand(m_db,cmd, res);
        CHECK_DB_ERR(m_ptr->m_con);
        //std::cout << "res: "<<res<<std::endl;
        if(!res["value"].isABSONObj())
        {
            if(m_verbose)
                std::cout << "No task available, cmd:" << cmd << std::endl;
            return false;
        }

        m_ptr->m_current_task = res["value"].Obj().copy();

        int timeout_s = INT_MAX;
        if(m_ptr->m_current_task.hasField("timeout"))
            timeout_s = m_ptr->m_current_task["timeout"].Int();

        m_ptr->m_current_task_timeout_time = now + boost::posix_time::seconds(timeout_s);
        m_ptr->m_running_nr = 0;

        o = m_ptr->m_current_task["misc"].Obj();

        // start logging
        m_ptr->m_log.clear();
        return true;
    }
    bool Client::get_best_task(mongo::BSONObj& task){
        mongo::BSONObjBuilder queryb;
        // select finished task
        queryb.append("state", TS_OK);
        if(! m_ptr->m_task_selector.isEmpty())
            queryb.appendElements(m_ptr->m_task_selector);

        // order by loss (ascending) and take first result
        std::auto_ptr<mongo::DBClientCursor> cursor = m_ptr->m_con.query(m_db + ".jobs",
                mongo::Query(queryb.obj()).sort("result.loss", 1), 1);

        if (!cursor->more()) {
            // no task found
            return false;
        }

        task = cursor->next();
        return true;
    }
    void Client::finish(const mongo::BSONObj& result, bool ok){
        const mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you finish!");
        }

        this->checkpoint(false); // flush logs, do not check for timeout

        boost::posix_time::ptime finish_time = universal_date_time();
        int version = ct["version"].Int();
        if(ok)
            m_ptr->m_con.update(m_jobcol,
                    QUERY("_id"<<ct["_id"]<<
                        "version"<<version),
                    BSON("$set"<<BSON(
                        "state"<<TS_OK<<
                        "version"<<version+1<<
                        "finish_time"<<to_mongo_date(finish_time)<<
                        "result"<<result)));
        else
            m_ptr->m_con.update(m_jobcol,
                    QUERY("_id"<<ct["_id"]<<
                        "version"<<version),
                    BSON("$set"<<BSON(
                        "state"<<TS_FAILED<<
                        "version"<<version+1<<
                        "failure_time"<<to_mongo_date(finish_time)<<
                        "result.status"<<"fail"<<
                        "error"<<result)));
        CHECK_DB_ERR(m_ptr->m_con);
        m_ptr->m_current_task = mongo::BSONObj(); // empty, call get_next_task.
    }
    void Client::reg(boost::asio::io_service& io_service, float interval){
        m_ptr->m_interval = interval;
        m_ptr->m_timer.reset(new boost::asio::deadline_timer(io_service, 
                    boost::posix_time::seconds(interval) + 
                    boost::posix_time::millisec((int)(1000*(interval-(int)interval)))));
        m_ptr->m_timer->async_wait(boost::bind(&ClientImpl::update_check, m_ptr.get(), this, boost::asio::placeholders::error));
    }
    
    void Client::handle_task(const mongo::BSONObj& o){
        std::cerr <<"MDBQC: WARNING: got a task, but no handler defined!"<<std::endl;
        finish(BSON("error"<<true));
    }
    Client::~Client(){ }
    void Client::log(int level, const mongo::BSONObj& msg){
        const mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you log something about it!");
        }
        boost::posix_time::ptime now = universal_date_time();
        m_ptr->m_log.push_back(BSON( 
                    mongo::GENOID<<
                    "taskid"<<ct["_id"]<<
                    "level"<<level<<
                    "nr" << m_ptr->m_running_nr++ <<
                    "timestamp"<< to_mongo_date(now)<<
                    "msg"<<msg));
    }
    void Client::log(int level, const char* ptr, size_t len, const mongo::BSONObj& msg){
        mongo::BSONObj& ct = m_ptr->m_current_task;
        if(ct.isEmpty()){
            throw std::runtime_error("MDBQC: get a task first before you log something about it!");
        }

        boost::uuids::basic_random_generator<boost::mt19937> gen;
        mongo::BSONObj ret = m_ptr->m_fs->storeFile(ptr,len, boost::lexical_cast<std::string>(gen()));
        CHECK_DB_ERR(m_ptr->m_con);
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
                    "level"<<level<<
                    "nr" << m_ptr->m_running_nr++ <<
                    "timestamp"<<to_mongo_date(universal_date_time())<<
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
                std::string hostname_pid = (boost::format("%s:%d") % &hostname[0] % getpid()).str();

                // set to failed in DB
                m_ptr->m_con.update(m_jobcol, 
                        QUERY("_id"<<ct["_id"] << 
                            // do not overwrite job that has been taken by someone else!
                            // this may happen due to timeouts and rescheduling.
                            "owner"<<hostname_pid),
                        BSON("$set" << 
                            BSON("state"<<TS_FAILED<< 
                                 "error"<<"timeout")));
                CHECK_DB_ERR(m_ptr->m_con);

                // clean up current state
                m_ptr->m_current_task = mongo::BSONObj();
                m_ptr->m_current_task_timeout_time = boost::posix_time::pos_infin;

                throw timeout_exception();
            }
        }

        boost::posix_time::ptime now = universal_date_time();
        m_ptr->m_con.update(m_jobcol,
                QUERY("_id"<<ct["_id"]),
                BSON( "$set"<<BSON("refresh_time"<<to_mongo_date(now))));
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
