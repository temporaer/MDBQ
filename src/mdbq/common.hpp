#ifndef __MDBQ_COMMON_HPP__
#     define __MDBQ_COMMON_HPP__

namespace mdbq
{
    enum TaskState{
        TS_NEW,
        TS_RUNNING,
        TS_OK,
        TS_FAILED
    };
}
#endif /* __MDBQ_COMMON_HPP__ */
