#ifndef __MDBQ_COMMON_HPP__
#     define __MDBQ_COMMON_HPP__

namespace mdbq
{
    enum TaskState{
        TS_OPEN,
        TS_ASSIGNED,
        TS_OK,
        TS_FAILED
    };
}
#endif /* __MDBQ_COMMON_HPP__ */
