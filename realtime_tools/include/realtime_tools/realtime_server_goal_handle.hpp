// Copyright (c) 2008, Willow Garage, Inc.
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
//    * Neither the name of the Willow Garage, Inc. nor the names of its
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

/// \author Stuart Glaser

#ifndef REALTIME_TOOLS__REALTIME_SERVER_GOAL_HANDLE_HPP_
#define REALTIME_TOOLS__REALTIME_SERVER_GOAL_HANDLE_HPP_

#include <atomic>
#include <memory>

#include "rclcpp/exceptions.hpp"
#include "rclcpp/logging.hpp"
#include "rclcpp_action/server_goal_handle.hpp"
#include "realtime_tools/realtime_double_buffer.hpp"

namespace realtime_tools
{

template <class Action>
class RealtimeServerGoalHandle
{
private:
  using GoalHandle = rclcpp_action::ServerGoalHandle<Action>;
  using ResultSharedPtr = typename Action::Result::SharedPtr;
  using FeedbackSharedPtr = typename Action::Feedback::SharedPtr;

  std::atomic<bool> req_abort_;
  std::atomic<bool> req_cancel_;
  std::atomic<bool> req_succeed_;
  std::atomic<bool> req_execute_;

  std::mutex mutex_;
  RealtimeDoubleBuffer<ResultSharedPtr> req_result_;
  // Only result written by non-RT thread
  ResultSharedPtr cancel_result_;
  RealtimeDoubleBuffer<FeedbackSharedPtr> req_feedback_;
  rclcpp::Logger logger_;

public:
  std::shared_ptr<GoalHandle> gh_;

  explicit RealtimeServerGoalHandle(std::shared_ptr<GoalHandle> & gh)
  : RealtimeServerGoalHandle(
      gh, /*preallocated_result, preallocated_feedback,*/ rclcpp::get_logger("realtime_tools"))
  {
  }

  RealtimeServerGoalHandle(std::shared_ptr<GoalHandle> & gh, rclcpp::Logger logger)
  : req_abort_(false),
    req_cancel_(false),
    req_succeed_(false),
    req_execute_(false),
    logger_(logger),
    gh_(gh)

  {
  }

  void setAborted(ResultSharedPtr result = nullptr)
  {
    if (
      req_execute_.load(std::memory_order_acquire) &&
      !req_succeed_.load(std::memory_order_acquire) &&
      !req_abort_.load(std::memory_order_acquire) && !req_cancel_.load(std::memory_order_acquire)) {
      req_result_.writeFromRT(result);
      req_abort_.store(true, std::memory_order_release);
    }
  }

  void setCanceled(ResultSharedPtr result = nullptr)
  {
    if (
      req_execute_.load(std::memory_order_acquire) &&
      !req_succeed_.load(std::memory_order_acquire) &&
      !req_abort_.load(std::memory_order_acquire) && !req_cancel_.load(std::memory_order_acquire)) {
      cancel_result_ = result;
      req_cancel_.store(true, std::memory_order_release);
    }
  }

  void setSucceeded(ResultSharedPtr result = nullptr)
  {
    if (
      req_execute_.load(std::memory_order_acquire) &&
      !req_succeed_.load(std::memory_order_acquire) &&
      !req_abort_.load(std::memory_order_acquire) && !req_cancel_.load(std::memory_order_acquire)) {
      req_result_.writeFromRT(result);
      req_succeed_.store(true, std::memory_order_release);
    }
  }

  void setFeedback(FeedbackSharedPtr feedback = nullptr) { req_feedback_.writeFromRT(feedback); }

  void execute()
  {
    if (
      !req_succeed_.load(std::memory_order_acquire) &&
      !req_abort_.load(std::memory_order_acquire) && !req_cancel_.load(std::memory_order_acquire)) {
      req_execute_.store(true, std::memory_order_release);
    }
  }

  bool valid() { return nullptr != gh_.get(); }

  void runNonRealtime()
  {
    if (!valid()) {
      return;
    }

    try {
      if (
        req_execute_.load(std::memory_order_acquire) && !gh_->is_executing() && gh_->is_active() &&
        !gh_->is_canceling()) {
        gh_->execute();
      }
      if (req_abort_.load(std::memory_order_acquire) && gh_->is_executing()) {
        gh_->abort(req_result_.readFromNonRT());
        req_abort_.store(false, std::memory_order_release);
      }
      if (req_cancel_.load(std::memory_order_acquire) && gh_->is_active()) {
        gh_->canceled(cancel_result_);
        req_cancel_.store(false, std::memory_order_release);
      }
      if (req_succeed_.load(std::memory_order_acquire) && !gh_->is_canceling()) {
        gh_->succeed(req_result_.readFromNonRT());
        req_succeed_.store(false, std::memory_order_release);
      }
      if (req_feedback_.readFromNonRT() && gh_->is_executing()) {
        gh_->publish_feedback(req_feedback_.readFromNonRT());
      }
    } catch (const rclcpp::exceptions::RCLErrorBase & e) {
      // Likely invalid state transition
      RCLCPP_WARN(logger_, "%s", e.formatted_message.c_str());
    }
  }
};

}  // namespace realtime_tools
#endif  // REALTIME_TOOLS__REALTIME_SERVER_GOAL_HANDLE_HPP_
