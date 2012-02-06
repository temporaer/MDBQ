#include <stdexcept>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <mongo/client/dbclient.h>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <mdbq/hub.hpp>
#include <mdbq/client.hpp>

using namespace mdbq;

TEST(hub, create_and_destroy){
    Hub hub("localhost", "test.gtest");
    hub.clear_all();
    EXPECT_EQ(hub.get_n_pending(), 0);
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    EXPECT_EQ(hub.get_n_pending(), 1);
}

TEST(hub, client_get_task){
    Hub    hub("localhost", "test.gtest");
    Client clt("localhost", "test.gtest");

    hub.clear_all();
    EXPECT_EQ(0, hub.get_n_pending());
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    EXPECT_EQ(1, hub.get_n_pending());
    EXPECT_EQ(0, hub.get_n_started());

    mongo::BSONObj task;
    EXPECT_TRUE(clt.get_next_task(task));
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    EXPECT_EQ(1, hub.get_n_started());
    EXPECT_EQ(1, hub.get_n_pending());
    EXPECT_EQ(1, task["foo"].Int());
    EXPECT_EQ(2, task["bar"].Int());

    clt.finish(BSON("baz"<<3));
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    hub.move_results_to_finished();
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    EXPECT_EQ(0, hub.get_n_pending());
    EXPECT_EQ(1, hub.get_n_finished());
}

TEST(hub, client_loop){
    Hub    hub("localhost", "test.gtest");
    Client clt("localhost", "test.gtest");

    hub.clear_all();
    EXPECT_EQ(hub.get_n_pending(), 0);
    hub.insert_job(BSON("foo"<<1<<"bar"<<2), 1000);
    EXPECT_EQ(hub.get_n_pending(), 1);
    EXPECT_EQ(hub.get_n_started(), 0);

    boost::asio::io_service io;
    clt.reg(io, 1);
    hub.reg(io, 1);
    io.run();
}
