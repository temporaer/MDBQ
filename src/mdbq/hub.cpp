#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <mongo/client/dbclient.h>
#include "common.hpp"
#include "hub.hpp"

namespace mdbq
{
    struct HubImpl{
        mongo::DBClientConnection m_con;

        unsigned int m_interval;
        std::string  m_prefix;
        std::auto_ptr<boost::asio::deadline_timer> m_timer;
        void update_check(Hub* c, const boost::system::error_code& error){

            // if a job takes too long, set it to `failed' so that client can stop working on it
            std::auto_ptr<mongo::DBClientCursor> p =
                m_con.query( m_prefix+"_jobs", 
                        QUERY(
                            "state"   << TS_ASSIGNED<<
                            "$where"  << "this.timeout+this.stime < "+boost::lexical_cast<std::string>(time(NULL))),
                        0,0,
                        &BSON("_id"<<1 << 
                            "nfailed"<<1)
                        );
            while(p->more()){
                mongo::BSONObj f = p->next();
                if(f["nfailed"].Int() < 1){// try again
                    m_con.update(m_prefix+"_jobs", 
                            QUERY("_id"<<f["_id"]), 
                            BSON(
                                "$inc" << BSON("nfailed"<<1)<<
                                "$set" << BSON("state"<<TS_OPEN <<"stime"<<-1)
                                ));
                }
                else{
                    m_con.update(m_prefix+"_jobs",  // set to failed
                            QUERY("_id"<<f["_id"]), 
                            BSON(
                                "$inc" << BSON("nfailed"<<1)<<
                                "$set" << BSON("state"<<TS_FAILED)
                                ));
                }
            }

            if(!error){
                m_timer->expires_at(m_timer->expires_at() + boost::posix_time::seconds(m_interval));
                m_timer->async_wait(boost::bind(&HubImpl::update_check,this,c,boost::asio::placeholders::error));
            }
        }
    };

    Hub::Hub(const std::string& url, const std::string& prefix)
        :m_prefix(prefix)
    {
        m_ptr.reset(new HubImpl());
        m_ptr->m_con.connect(url);
        m_ptr->m_con.createCollection(prefix+"_jobs");
        m_ptr->m_prefix = prefix;
    }

    void Hub::insert_job(const mongo::BSONObj& job, unsigned int timeout){
        long long int ctime = time(NULL);
        long long int to    = timeout;
        m_ptr->m_con.insert(m_prefix+"_jobs", 
                BSON( mongo::GENOID
                    <<"timeout"  << to
                    <<"ctime"  << ctime
                    <<"ftime"  << -1
                    <<"stime"  << -1
                    <<"payload"  <<job
                    <<"nfailed"  <<0
                    <<"state"    <<TS_OPEN
                    )
                );
    }
    size_t Hub::get_n_open(){
        return m_ptr->m_con.count(m_prefix+"_jobs", 
                BSON( "state" << TS_OPEN));
    }
    size_t Hub::get_n_assigned(){
        return m_ptr->m_con.count(m_prefix+"_jobs", 
                BSON( "state" << TS_ASSIGNED));
    }
    size_t Hub::get_n_ok(){
        return m_ptr->m_con.count(m_prefix+"_jobs", 
                BSON( "state" << TS_OK));
    }
    size_t Hub::get_n_failed(){
        return m_ptr->m_con.count(m_prefix+"_jobs", 
                BSON( "state" << TS_FAILED));
    }
    void Hub::clear_all(){
        m_ptr->m_con.dropCollection(m_prefix+"_jobs");
        m_ptr->m_con.dropCollection(m_prefix+"_finished");
    }
    /*
     *size_t Hub::move_results_to_finished(){
     *    std::auto_ptr<mongo::DBClientCursor> p =
     *        m_ptr->m_con.query( m_prefix+"_jobs",
     *                QUERY("finished" << mongo::GT <<  0));
     *    unsigned int cnt=0;
     *    while(p->more()){
     *        mongo::BSONObj f = p->next();
     *        m_ptr->m_con.remove( m_prefix+"_jobs",
     *                QUERY( "_id"<<f["_id"].OID() ));
     *        m_ptr->m_con.insert( m_prefix+"_finished",
     *                f);
     *        cnt++;
     *    }
     *    return cnt;
     *}
     */
    void Hub::got_new_results(){
        std::cout <<"New results available!"<<std::endl;
    }

    void Hub::reg(boost::asio::io_service& io_service, unsigned int interval){
        m_ptr->m_interval = interval;
        m_ptr->m_timer.reset(new boost::asio::deadline_timer(io_service, boost::posix_time::seconds(interval)));
        m_ptr->m_timer->async_wait(boost::bind(&HubImpl::update_check, m_ptr.get(), this, boost::asio::placeholders::error));
    }

    mongo::BSONObj Hub::get_newest_finished(){
        return m_ptr->m_con.findOne(m_prefix+"_jobs",
                QUERY("state"<<TS_OK).sort("ftime"));
    }
}

