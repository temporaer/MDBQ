#ifndef __MDBQ_CLIENT_HPP__
#     define __MDBQ_CLIENT_HPP__

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
    struct ClientImpl;
    class Client{
        private:
            boost::shared_ptr<ClientImpl> m_ptr;
            const std::string m_prefix;
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

            void log(const mongo::BSONObj& msg);

            void checkpoint();

            virtual void handle_task(const mongo::BSONObj& task);
            
            virtual ~Client();
    };
}
#endif /* __MDBQ_CLIENT_HPP__ */
