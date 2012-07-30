#ifndef __MDBQ_CLIENT_HPP__
#     define __MDBQ_CLIENT_HPP__

#include <stdexcept>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <string>

namespace mongo{
    class BSONObj;
}
namespace boost{
    namespace asio
    {
        class io_service;
    }
}

namespace mdbq
{

    class timeout_exception : public std::runtime_error{
        public:
            timeout_exception() : std::runtime_error("MDBQ Timeout") {}
    };


    struct ClientImpl;
    class Client{
        private:
            boost::shared_ptr<ClientImpl> m_ptr;
            std::string m_jobcol;
            std::string m_logcol;
            std::string m_fscol;
            std::string m_db;
        public:
            Client(const std::string& url, const std::string& prefix);
            bool get_next_task(mongo::BSONObj& o);
            void finish(const mongo::BSONObj& result, bool ok=1);

            /**
             * register with the main loop
             *
             * @param interval querying interval
             */
            void reg(boost::asio::io_service& io_service, unsigned int interval);

            /**
             * log a bson obj directly with the job
             */
            void log(int level, const mongo::BSONObj& msg);

            /**
             * log a file to gridfs, /refer/ to it in job log
             */
            void log(int level, const char* ptr, size_t len, const mongo::BSONObj& msg);

            /**
             * get the log of a task (mainly for testing)
             */
            std::vector<mongo::BSONObj> get_log(const mongo::BSONObj& task);

            /**
             * flush logs and check for timeouts (throws timeout_exception)
             */
            void checkpoint(bool check_for_timeout=true);

            virtual void handle_task(const mongo::BSONObj& task);
            
            virtual ~Client();
    };
}
#endif /* __MDBQ_CLIENT_HPP__ */
