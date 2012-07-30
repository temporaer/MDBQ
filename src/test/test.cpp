#include <stdexcept>
#include <mongo/client/dbclient.h>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <mdbq/hub.hpp>
#include <mdbq/client.hpp>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE MdbQ
#include <boost/test/unit_test.hpp>


using namespace mdbq;

#define HOST "131.220.7.92"

struct Fix{
    Hub hub;
    Client clt;
    Fix()
        :hub(HOST,"test.mdbq")
        ,clt(HOST,"test.mdbq")
    {
        hub.clear_all();
    }
};

BOOST_FIXTURE_TEST_SUITE(MdbQ, Fix)
BOOST_AUTO_TEST_CASE(create_and_destroy){
    BOOST_CHECK_EQUAL(hub.get_n_open(), 0);
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(hub.get_n_open(), 1);
}

BOOST_AUTO_TEST_CASE(client_get_task){
    BOOST_CHECK_EQUAL(0, hub.get_n_open());
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(1, hub.get_n_open());
    BOOST_CHECK_EQUAL(0, hub.get_n_assigned());

    mongo::BSONObj task;
    BOOST_CHECK(clt.get_next_task(task));
    BOOST_CHECK(!task.isEmpty());
    BOOST_CHECK_EQUAL(1, hub.get_n_assigned());
    BOOST_CHECK_EQUAL(0, hub.get_n_open());
    BOOST_CHECK_EQUAL(1, task["foo"].Int());
    BOOST_CHECK_EQUAL(2, task["bar"].Int());

    clt.finish(BSON("baz"<<3));
    BOOST_CHECK_EQUAL(0, hub.get_n_open());
    BOOST_CHECK_EQUAL(1, hub.get_n_ok());
}

BOOST_AUTO_TEST_CASE(logging){
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(1, hub.get_n_open());
    BOOST_CHECK_EQUAL(0, hub.get_n_assigned());

    mongo::BSONObj task;
    BOOST_CHECK(clt.get_next_task(task));
    BOOST_CHECK(!task.isEmpty());
    clt.log(BSON("level"<<0<<"num"<<1));
    clt.log(BSON("level"<<0<<"num"<<2));
    clt.checkpoint();
    clt.log(BSON("level"<<0<<"num"<<3));
    clt.checkpoint();

    clt.finish(BSON("baz"<<3));
    mongo::BSONObj t = hub.get_newest_finished();
    std::vector<mongo::BSONObj> log = clt.get_log(t);
    BOOST_CHECK_EQUAL(t["result"]["baz"].Int(), 3);
    BOOST_CHECK_EQUAL(log[0]["msg"]["level"].Int(), 0);
    BOOST_CHECK_EQUAL(log[0]["msg"]["num"].Int(), 1);
    BOOST_CHECK_EQUAL(log[1]["msg"]["level"].Int(), 0);
    BOOST_CHECK_EQUAL(log[1]["msg"]["num"].Int(), 2);
    BOOST_CHECK_EQUAL(log[2]["msg"]["level"].Int(), 0);
    BOOST_CHECK_EQUAL(log[2]["msg"]["num"].Int(), 3);
}

BOOST_AUTO_TEST_CASE(client_loop){
    BOOST_CHECK_EQUAL(hub.get_n_open(), 0);
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    BOOST_CHECK_EQUAL(hub.get_n_open(), 1);
    BOOST_CHECK_EQUAL(hub.get_n_assigned(), 0);

    boost::asio::io_service io;
    clt.reg(io, 1);
    hub.reg(io, 1);

    boost::asio::deadline_timer dt(io, boost::posix_time::seconds(4));
    dt.async_wait(boost::bind(&boost::asio::io_service::stop, &io));

    io.run();

    BOOST_CHECK_EQUAL(1, hub.get_n_ok());
}

struct work_forever_client
: public Client{
    work_forever_client(std::string a, std::string b): Client(a,b),caught(0){}
    int caught;
    void handle_task(const mongo::BSONObj& o){
        try{
            while(true){
                boost::this_thread::sleep(boost::posix_time::seconds(1));
                checkpoint();
            }
        }catch(timeout_exception){
            std::cout <<"TEST: work_forever_client got timeout exception. Surprise."<<std::endl;
            caught++;
        }
    }
};

struct work_a_bit_client
: public Client{
    work_a_bit_client(std::string a, std::string b): Client(a,b),i(0){}
    unsigned int i;
    void handle_task(const mongo::BSONObj& o){
        try{
            boost::this_thread::sleep(boost::posix_time::seconds(0.01));
            log(BSON("logging"<<i++));
            checkpoint();
            const char* s = "iafgiauhf iwu hfiuwh fpiuqwh feipuhweoifuh iwufeh iwufh 3q4uhf ";
            log(s,strlen(s),BSON("logging"<<i++));
            checkpoint();
            finish(BSON("done"<<1));
        }catch(timeout_exception){
            std::cout <<"work_a_bit_client got timeout exception."<<std::endl;
        }
    }
};

BOOST_AUTO_TEST_CASE(timeouts){
    BOOST_CHECK_EQUAL(hub.get_n_open(), 0);
    hub.insert_job(BSON("timeouts_test"<<1<<"bar"<<2), 1);
    BOOST_CHECK_EQUAL(hub.get_n_open(), 1);
    BOOST_CHECK_EQUAL(hub.get_n_assigned(), 0);
    BOOST_CHECK_EQUAL(hub.get_n_failed(), 0);

    boost::asio::io_service hub_io, clt_io;
    work_forever_client wfc(HOST,"test.mdbq");
    wfc.reg(clt_io, 1);
    hub.reg(hub_io, 1);

    boost::asio::deadline_timer hub_dt(hub_io, boost::posix_time::seconds(12));
    hub_dt.async_wait(boost::bind(&boost::asio::io_service::stop, &hub_io));

    boost::asio::deadline_timer clt_dt(clt_io, boost::posix_time::seconds(7));
    clt_dt.async_wait(boost::bind(&boost::asio::io_service::stop, &clt_io));

    // start client thread
    boost::thread clt_thread(boost::bind(&boost::asio::io_service::run, &clt_io));
    
    hub_io.run();
    clt_thread.join();

    BOOST_CHECK_EQUAL(wfc.caught, 2); // should be rescheduled and fail a 2nd time
    BOOST_CHECK_EQUAL(1, hub.get_n_failed());
}

BOOST_AUTO_TEST_CASE(hardcore){
    boost::asio::io_service hub_io, clt1_io, clt2_io;
    unsigned int n_jobs = 1000;
    for (int i = 0; i < n_jobs; ++i)
    {
        hub.insert_job(BSON("foo"<<i<<"bar"<<i), 1);
    }
    work_a_bit_client wabc1(HOST,"test.mdbq");
    work_a_bit_client wabc2(HOST,"test.mdbq");
    wabc1.reg(clt1_io,0.01);
    wabc2.reg(clt2_io,0.01);
    hub.reg(hub_io, 0.1);

    boost::asio::deadline_timer hub_dt(hub_io, boost::posix_time::seconds(20));
    hub_dt.async_wait(boost::bind(&boost::asio::io_service::stop, &hub_io));

    boost::asio::deadline_timer clt1_dt(clt1_io, boost::posix_time::seconds(20));
    clt1_dt.async_wait(boost::bind(&boost::asio::io_service::stop, &clt1_io));

    boost::asio::deadline_timer clt2_dt(clt2_io, boost::posix_time::seconds(20));
    clt2_dt.async_wait(boost::bind(&boost::asio::io_service::stop, &clt2_io));

    // start client thread
    boost::thread clt1_thread(boost::bind(&boost::asio::io_service::run, &clt1_io));
    boost::thread clt2_thread(boost::bind(&boost::asio::io_service::run, &clt2_io));
    
	std::cout << "TEST: Starting many-job-test, takes 20s to finish" << std::endl;
    hub_io.run();
    clt1_thread.join();
    clt2_thread.join();
    BOOST_CHECK_EQUAL(n_jobs, hub.get_n_ok());
}

BOOST_AUTO_TEST_CASE(filestorage){
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1);
    mongo::BSONObj task;
    clt.get_next_task(task);
    const char* s = "hallihallohallihallohallihallohallihallohallihallo";
    clt.log(s, strlen(s), BSON("baz"<<3));
    clt.finish(BSON("baz"<<4));
}
BOOST_AUTO_TEST_SUITE_END()
