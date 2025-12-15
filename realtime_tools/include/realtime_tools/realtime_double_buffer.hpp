// Copyright (c) 2013, hiDOF, Inc.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the hiDOF, Inc. nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

/*
 * Author: Marek Daniv
 */

#ifndef REALTIME_TOOLS__REALTIME_DOUBLE_BUFFER_HPP_
#define REALTIME_TOOLS__REALTIME_DOUBLE_BUFFER_HPP_

#include <chrono>
#include <mutex>
#include <thread>

namespace realtime_tools
{
template <class T>
class RealtimeDoubleBuffer
{
public:
  RealtimeDoubleBuffer() {}

  /**
   * @brief Constructor for objects that don't have
   * a default constructor
   * @param data The object to use as default value
   */
  explicit RealtimeDoubleBuffer(const T & data)
  {
    for (size_t i = 0; i < size_; ++i) {
      buffer_[i] = data;
    }
  };

  ~RealtimeDoubleBuffer() {}

  RealtimeDoubleBuffer(const RealtimeDoubleBuffer & source)
  {
    for (size_t i = 0; i < size_; ++i) {
      buffer_[i] = source.buffer_[i];
    }
  }

  /*!
   * @brief Custom assignment operator
   */
  RealtimeDoubleBuffer & operator=(const RealtimeDoubleBuffer & source)
  {
    if (this == &source) {
      return *this;
    }

    for (size_t i = 0; i < size_; ++i) {
      buffer_[i] = source.buffer_[i];
    }

    return *this;
  }

  void initRT(const T & data)
  {
    for (size_t i = 0; i < size_; ++i) {
      buffer_[i] = data;
    }
  };

  void writeFromRT(const T & data)
  {
    size_t write_index = (last_write_.load(std::memory_order_acquire) + 1) % size_;

    while (true) {
      std::unique_lock<std::mutex> guard(mutexes_[write_index], std::try_to_lock);
      if (guard.owns_lock()) {
        buffer_[write_index] = data;
        last_write_.store(write_index, std::memory_order_release);
        break;
      } else {
        write_index = (write_index + 1) % size_;
      }
    }
  };

  T readFromRT()
  {
    size_t read_index = last_write_.load(std::memory_order_acquire);
    return buffer_[read_index];
  };

  T readFromNonRT() const
  {
    size_t read_index = last_write_.load(std::memory_order_acquire);

#ifdef NON_POLLING
    std::lock_guard<std::mutex> guard(mutexes_[read_index]);
#else
    std::unique_lock<std::mutex> guard(mutexes_[read_index], std::defer_lock);
    while (!guard.try_lock()) {
      std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
#endif

    return buffer_[read_index];
  };

private:
  static constexpr size_t size_ = 2;
  mutable std::array<std::mutex, size_> mutexes_;
  std::array<T, size_> buffer_;

  std::atomic<size_t> last_write_ = 0;
};
}  // namespace realtime_tools

#endif  // REALTIME_TOOLS__REALTIME_DOUBLE_BUFFER_HPP_
