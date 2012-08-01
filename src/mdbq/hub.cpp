#include <iomanip>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <mongo/client/dbclient.h>
#include "common.hpp"
#include "hub.hpp"
#include "date_time.hpp"

#ifdef NDEBUG
#  define CHECK_DB_ERR(CON)
#else
#  define CHECK_DB_ERR(CON)\
            {\
                string e = (CON).getLastError();\
                if(!e.empty()){\
                    throw std::runtime_error("hub: error_code!=0, failing: " + e + "\n" + (CON).getLastErrorDetailed().toString() );\
                }\
            }
#endif

namespace mdbq
{
    struct HubImpl{
        mongo::DBClientConnection m_con;

        unsigned int m_interval;
        std::string  m_prefix;
        std::auto_ptr<boost::asio::deadline_timer> m_timer;
        void print_current_job_summary(Hub* c, const boost::system::error_code& error){
            std::auto_ptr<mongo::DBClientCursor> p =
                // note: currently snapshot mode may not be used w/ sorting or explicit hints
                m_con.query( m_prefix+".jobs", 
                        QUERY( "state"   << mongo::GT<< -1));
            CHECK_DB_ERR(m_con);

            std::cout << "JOB SUMMARY" << std::endl;
            std::cout << "===========" << std::endl
                    << "Type    : "
                    << std::setw(8)<<"state"
                    << std::setw(8) <<"nfailed"
                    << std::setw(10) <<"owner"
                    << std::setw(16) <<"create_time"
                    << std::setw(16)<<"book_time"
                    << std::setw(16)<<"finish_time"
                    << std::setw(16)<<"deadline"
                    << std::setw(16)<<"misc"
                    << std::endl;
            while(p->more()){
                mongo::BSONObj f = p->next();
                if(f.isEmpty())
                    continue;
                std::cout << "ASSIGNED: "
                    << std::setw(8) <<f["state"].Int()
                    << std::setw(8) <<f["nfailed"].Int()
                    << std::setw(10) <<f["owner"].Array()[0].String()
                    << std::setw(16)<<dt_format(to_ptime(f["create_time"].Date()))
                    << std::setw(16)<<dt_format(to_ptime(f["book_time"].Date()))
                    << std::setw(16)<<dt_format(to_ptime(f["finish_time"].Date()))
                    << std::setw(16)<<dt_format(to_ptime(f["book_time"].Date())+boost::posix_time::seconds(f["timeout"].Int()))
                    << " " << f["misc"]
                    << std::endl;
            }
        }
        void update_check(Hub* c, const boost::system::error_code& error){
            //print_current_job_summary(c,error);

            // search for jobs which have failed and reschedule them
            mongo::BSONObj ret = BSON("_id"<<1 << 
                           "owner"<<1 <<
                           "nfailed"<<1);
            std::auto_ptr<mongo::DBClientCursor> p =
               m_con.query( m_prefix+".jobs", 
                       QUERY(
                           "state" << TS_FAILED <<
                           "nfailed" << mongo::LT << 1), /* first time failure only */
                       0,0,&ret);
            CHECK_DB_ERR(m_con);
            while(p->more()){
                mongo::BSONObj f = p->next();
                if(!f.hasField("nfailed"))
                    continue;

                std::cerr << "HUB: warning: task `"
                    << f["_id"] << "' on `"
                    << f["owner"].String() << "' failed, rescheduling"<<std::endl;

                m_con.update(m_prefix+".jobs", 
                        QUERY("_id"<<f["_id"]), 
                        BSON(
                            "$inc" << BSON("nfailed"<<1)<<
                            "$set" << BSON(
                                "state"         << TS_NEW 
                                <<"book_time"   << mongo::Undefined
                                <<"refresh_time"<< mongo::Undefined)));
                CHECK_DB_ERR(m_con);
            }

            if(!error){
                m_timer->expires_at(m_timer->expires_at() + boost::posix_time::seconds(m_interval));
                m_timer->async_wait(boost::bind(&HubImpl::update_check,this,c,boost::asio::placeholders::error));
            }else{
                throw std::runtime_error("HUB: error_code!=0, failing!");
            }
        }
    };

    Hub::Hub(const std::string& url, const std::string& prefix)
        :m_prefix(prefix)
    {
        m_ptr.reset(new HubImpl());
        m_ptr->m_con.connect(url);
        m_ptr->m_con.createCollection(prefix+".jobs");
        m_ptr->m_prefix = prefix;
    }

    void Hub::insert_job(const mongo::BSONObj& job, unsigned int timeout, const std::string& driver){
        boost::posix_time::ptime ctime = universal_date_time();
        m_ptr->m_con.insert(m_prefix+".jobs", 
                BSON( mongo::GENOID
                    <<"timeout"     << timeout
                    <<"exp_key"     << driver
                    <<"create_time" << to_mongo_date(ctime)
                    <<"finish_time" << mongo::Undefined
                    <<"book_time"   << mongo::Undefined
                    <<"refresh_time"<< mongo::Undefined
                    <<"misc"        << job
                    <<"nfailed"     << (int)0
                    <<"state"       << TS_NEW
                    <<"result"      << BSON("status"<<"new")
                    )
                );
        CHECK_DB_ERR(m_ptr->m_con);
    }
    size_t Hub::get_n_open(){
        return m_ptr->m_con.count(m_prefix+".jobs", 
                BSON( "state" << TS_NEW));
    }
    size_t Hub::get_n_assigned(){
        return m_ptr->m_con.count(m_prefix+".jobs", 
                BSON( "state" << TS_RUNNING));
    }
    size_t Hub::get_n_ok(){
        return m_ptr->m_con.count(m_prefix+".jobs", 
                BSON( "state" << TS_OK));
    }
    size_t Hub::get_n_failed(){
        return m_ptr->m_con.count(m_prefix+".jobs", 
                BSON( "state" << TS_FAILED));
    }
    void Hub::clear_all(){
        m_ptr->m_con.dropCollection(m_prefix+".jobs");
        m_ptr->m_con.dropCollection(m_prefix+".log");
        m_ptr->m_con.dropCollection(m_prefix+".fs.chunks");
        m_ptr->m_con.dropCollection(m_prefix+".fs.files");
    }
    void Hub::got_new_results(){
        std::cout <<"New results available!"<<std::endl;
    }

    void Hub::reg(boost::asio::io_service& io_service, unsigned int interval){
        m_ptr->m_interval = interval;
        m_ptr->m_timer.reset(new boost::asio::deadline_timer(io_service, boost::posix_time::seconds(interval)));
        m_ptr->m_timer->async_wait(boost::bind(&HubImpl::update_check, m_ptr.get(), this, boost::asio::placeholders::error));
    }

    mongo::BSONObj Hub::get_newest_finished(){
        return m_ptr->m_con.findOne(m_prefix+".jobs",
                QUERY("state"<<TS_OK).sort("finish_time"));
    }
}

