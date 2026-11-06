// Copyright (c) 2019, Open Source Robotics Foundation, Inc.
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
//    * Neither the name of the Open Source Robotics Foundation, Inc. nor the names of its
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

#include <gmock/gmock.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rclcpp/executors.hpp"
#include "rclcpp/utilities.hpp"
#include "rclcpp_action/create_client.hpp"
#include "rclcpp_action/create_server.hpp"
#include "realtime_tools/realtime_server_goal_handle.hpp"
#include "test_msgs/action/fibonacci.hpp"

using test_msgs::action::Fibonacci;
using ServerGoalHandle = rclcpp_action::ServerGoalHandle<Fibonacci>;
using ClientGoalHandle = rclcpp_action::ClientGoalHandle<Fibonacci>;
using realtime_tools::RealtimeServerGoalHandle;

const size_t ATTEMPTS = 10;
const std::chrono::milliseconds DELAY(250);

struct ActionServerCallbacks
{
  bool have_handle_ = false;
  std::shared_ptr<ServerGoalHandle> handle_;
  std::mutex mtx_;

  rclcpp_action::GoalResponse goal_callback(
    const rclcpp_action::GoalUUID &, std::shared_ptr<const Fibonacci::Goal>)
  {
    return rclcpp_action::GoalResponse::ACCEPT_AND_DEFER;
  }

  void accepted_callback(std::shared_ptr<ServerGoalHandle> handle)
  {
    std::unique_lock<std::mutex> lock(mtx_);
    handle_ = handle;
    have_handle_ = true;
  }

  rclcpp_action::CancelResponse cancel_callback(std::shared_ptr<ServerGoalHandle>)
  {
    return rclcpp_action::CancelResponse::ACCEPT;
  }

  bool wait_for_handle(rclcpp::Node::SharedPtr node)
  {
    rclcpp::executors::SingleThreadedExecutor executor;
    executor.add_node(node);
    for (size_t i = 0; i < ATTEMPTS; ++i) {
      executor.spin_some();
      std::this_thread::sleep_for(DELAY);
      std::unique_lock<std::mutex> lock(mtx_);
      if (have_handle_) {
        break;
      }
    }
    std::unique_lock<std::mutex> lock(mtx_);
    return have_handle_;
  }
};

struct ActionClientCallbacks
{
  bool have_feedback_ = false;
  bool have_result_ = false;
  std::vector<int32_t> feedback_sequence_;
  std::mutex mtx_;

  void feedback_callback(
    ClientGoalHandle::SharedPtr, const Fibonacci::Feedback::ConstSharedPtr & feedback)
  {
    std::unique_lock<std::mutex> lock(mtx_);
    feedback_sequence_ = feedback->sequence;
    have_feedback_ = true;
  }

  void result_callback(const ClientGoalHandle::WrappedResult &)
  {
    std::unique_lock<std::mutex> lock(mtx_);
    have_result_ = true;
  }

  bool wait_for_feedback(rclcpp::Node::SharedPtr node)
  {
    rclcpp::executors::SingleThreadedExecutor executor;
    executor.add_node(node);
    for (size_t i = 0; i < ATTEMPTS; ++i) {
      executor.spin_some();
      std::this_thread::sleep_for(DELAY);
      std::unique_lock<std::mutex> lock(mtx_);
      if (have_feedback_) {
        break;
      }
    }
    std::unique_lock<std::mutex> lock(mtx_);
    return have_feedback_;
  }

  std::vector<int32_t> feedback_sequence()
  {
    std::unique_lock<std::mutex> lock(mtx_);
    return feedback_sequence_;
  }
};

std::shared_ptr<ClientGoalHandle> send_goal(
  rclcpp::Node::SharedPtr node, std::shared_ptr<rclcpp_action::Client<Fibonacci>> ac,
  const std::string & /*server_name*/, ActionClientCallbacks & client_callbacks)
{
  {
    rclcpp::executors::SingleThreadedExecutor executor;
    executor.add_node(node);
    for (size_t i = 0; i < ATTEMPTS && !ac->action_server_is_ready(); ++i) {
      executor.spin_some();
      std::this_thread::sleep_for(DELAY);
    }
  }

  if (ac->action_server_is_ready()) {
    Fibonacci::Goal goal;
    goal.order = 10;
    // Create "SendGoalOptions" that has callbacks set
    rclcpp_action::Client<Fibonacci>::SendGoalOptions send_goal_options;
    send_goal_options.feedback_callback = std::bind(
      &ActionClientCallbacks::feedback_callback, &client_callbacks, std::placeholders::_1,
      std::placeholders::_2);
    send_goal_options.result_callback =
      std::bind(&ActionClientCallbacks::result_callback, &client_callbacks, std::placeholders::_1);
    // wait to get a client handle
    auto future_handle = ac->async_send_goal(goal, send_goal_options);
    rclcpp::spin_until_future_complete(node, future_handle);
    return future_handle.get();
  }
  return nullptr;
}

std::shared_ptr<ClientGoalHandle::WrappedResult> wait_for_result(
  rclcpp::Node::SharedPtr node, std::shared_ptr<ClientGoalHandle> client_goal_handle,
  std::shared_ptr<rclcpp_action::Client<Fibonacci>> client)
{
  // Get a result future
  auto result_future = client->async_get_result(client_goal_handle);
  if (
    rclcpp::spin_until_future_complete(node, result_future, ATTEMPTS * DELAY) !=
    rclcpp::FutureReturnCode::SUCCESS) {
    return nullptr;
  }
  return std::make_shared<ClientGoalHandle::WrappedResult>(result_future.get());
}

bool cancel_and_wait(
  rclcpp::Node::SharedPtr node, std::shared_ptr<rclcpp_action::Client<Fibonacci>> client,
  std::shared_ptr<ClientGoalHandle> client_goal_handle, ActionServerCallbacks & callbacks)
{
  client->async_cancel_goal(client_goal_handle);
  for (size_t i = 0; i < ATTEMPTS; ++i) {
    if (callbacks.handle_->is_canceling()) {
      break;
    }
    rclcpp::executors::SingleThreadedExecutor exec;
    exec.add_node(node);
    exec.spin_some();
    std::this_thread::sleep_for(DELAY);
  }
  return callbacks.handle_->is_canceling();
}

rclcpp_action::Server<Fibonacci>::SharedPtr make_server(
  rclcpp::Node::SharedPtr node, const std::string & server_name, ActionServerCallbacks & callbacks)
{
  return rclcpp_action::create_server<Fibonacci>(
    node, server_name,
    std::bind(
      &ActionServerCallbacks::goal_callback, &callbacks, std::placeholders::_1,
      std::placeholders::_2),
    std::bind(&ActionServerCallbacks::cancel_callback, &callbacks, std::placeholders::_1),
    std::bind(&ActionServerCallbacks::accepted_callback, &callbacks, std::placeholders::_1));
}

TEST(RealtimeServerGoalHandle, set_aborted)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("set_aborted");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/set_aborted");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
  ASSERT_TRUE(rt_handle.valid());
  rt_handle.execute();
  rt_handle.runNonRealtime();

  {
    auto result = std::make_shared<Fibonacci::Result>();
    result->sequence = {1, 1, 2, 3, 5};
    rt_handle.setAborted(result);
    rt_handle.runNonRealtime();
  }

  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::ABORTED);
  ASSERT_NE(nullptr, wrapped_result->result.get());
  EXPECT_EQ(5, wrapped_result->result->sequence.size());
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, set_canceled)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("set_canceled");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/set_canceled");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
  ASSERT_TRUE(rt_handle.valid());
  rt_handle.execute();
  rt_handle.runNonRealtime();

  ASSERT_TRUE(cancel_and_wait(node, client, client_handle, callbacks));

  {
    auto result = std::make_shared<Fibonacci::Result>();
    result->sequence = {1, 1, 2, 3, 5};
    rt_handle.setCanceled(result);
    rt_handle.runNonRealtime();
  }

  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::CANCELED);
  ASSERT_NE(nullptr, wrapped_result->result.get());
  EXPECT_EQ(5, wrapped_result->result->sequence.size());
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, set_succeeded)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("set_succeeded");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/set_succeeded");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
  ASSERT_TRUE(rt_handle.valid());
  rt_handle.execute();
  rt_handle.runNonRealtime();

  {
    auto result = std::make_shared<Fibonacci::Result>();
    result->sequence = {1, 1, 2, 3, 5};
    rt_handle.setSucceeded(result);
    rt_handle.runNonRealtime();
  }

  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::SUCCEEDED);
  ASSERT_NE(nullptr, wrapped_result->result.get());
  EXPECT_EQ(5, wrapped_result->result->sequence.size());
  rclcpp::shutdown();
}

// The action server may finish a goal in any way while a cancel request is pending
TEST(RealtimeServerGoalHandle, set_succeeded_or_aborted_while_canceling)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("set_while_canceling");
  const std::string server_name("~/client/set_while_canceling");
  ActionServerCallbacks callbacks;
  auto as = make_server(node, server_name, callbacks);
  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);

  for (const auto code :
       {rclcpp_action::ResultCode::SUCCEEDED, rclcpp_action::ResultCode::ABORTED}) {
    callbacks.have_handle_ = false;
    ActionClientCallbacks client_callbacks;
    auto client_handle = send_goal(node, client, server_name, client_callbacks);
    ASSERT_NE(nullptr, client_handle.get());
    ASSERT_TRUE(callbacks.wait_for_handle(node));
    RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
    rt_handle.execute();
    rt_handle.runNonRealtime();
    ASSERT_TRUE(cancel_and_wait(node, client, client_handle, callbacks));

    auto result = std::make_shared<Fibonacci::Result>();
    result->sequence = {1, 1, 2, 3, 5};
    if (code == rclcpp_action::ResultCode::SUCCEEDED) {
      rt_handle.setSucceeded(result);
    } else {
      rt_handle.setAborted(result);
    }
    rt_handle.runNonRealtime();

    auto wrapped_result = wait_for_result(node, client_handle, client);
    ASSERT_NE(nullptr, wrapped_result.get());
    EXPECT_EQ(wrapped_result->code, code);
    ASSERT_NE(nullptr, wrapped_result->result.get());
    EXPECT_EQ(5, wrapped_result->result->sequence.size());
  }
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, set_aborted_without_result_sends_preallocated_result)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("set_aborted_without_result");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/set_aborted_without_result");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  auto preallocated_result = std::make_shared<Fibonacci::Result>();
  preallocated_result->sequence = {1, 1, 2};
  RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_, preallocated_result);
  rt_handle.execute();
  rt_handle.runNonRealtime();
  rt_handle.setAborted();
  rt_handle.runNonRealtime();

  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::ABORTED);
  ASSERT_NE(nullptr, wrapped_result->result.get());
  EXPECT_THAT(wrapped_result->result->sequence, testing::ElementsAre(1, 1, 2));
  rclcpp::shutdown();
}

// rclcpp_action aborts a goal whose last handle is dropped before it finished
TEST(RealtimeServerGoalHandle, destroy_while_executing_without_result)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("destroy_without_result");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/destroy_without_result");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  {
    RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
    rt_handle.execute();
    rt_handle.runNonRealtime();
    callbacks.handle_.reset();
  }

  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::ABORTED);
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, destroy_with_pending_result_sends_it)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("destroy_with_pending_result");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/destroy_with_pending_result");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  {
    RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
    rt_handle.execute();
    rt_handle.runNonRealtime();
    auto result = std::make_shared<Fibonacci::Result>();
    result->sequence = {1, 1, 2, 3, 5};
    rt_handle.setSucceeded(result);
    callbacks.handle_.reset();
  }

  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::SUCCEEDED);
  ASSERT_NE(nullptr, wrapped_result->result.get());
  EXPECT_EQ(5, wrapped_result->result->sequence.size());
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, destroy_survives_failing_result_publish)
{
  rclcpp::init(0, nullptr);
  // Only the node's own context goes down, so the destructor still sends the pending result
  auto context = std::make_shared<rclcpp::Context>();
  context->init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>(
    "destroy_failing_publish", rclcpp::NodeOptions().context(context));
  rclcpp::ExecutorOptions executor_options;
  executor_options.context = context;
  rclcpp::executors::SingleThreadedExecutor executor(executor_options);
  executor.add_node(node);
  ActionServerCallbacks callbacks;
  const std::string server_name("~/client/destroy_failing_publish");
  auto as = make_server(node, server_name, callbacks);
  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  ASSERT_TRUE(client->wait_for_action_server(ATTEMPTS * DELAY));
  auto goal_future = client->async_send_goal(Fibonacci::Goal());
  ASSERT_EQ(
    rclcpp::FutureReturnCode::SUCCESS,
    executor.spin_until_future_complete(goal_future, ATTEMPTS * DELAY));
  ASSERT_NE(nullptr, callbacks.handle_.get());

  {
    RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
    rt_handle.execute();
    rt_handle.runNonRealtime();
    rt_handle.setSucceeded();
    context->shutdown("publishing the result fails from now on");
  }
  EXPECT_FALSE(callbacks.handle_->is_active());
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, set_result_does_not_wait_for_lock)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("set_result_does_not_wait_for_lock");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/set_result_does_not_wait_for_lock");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
  rt_handle.execute();
  rt_handle.runNonRealtime();

  std::atomic<bool> lock_held = false;
  std::atomic<bool> set_returned = false;
  bool set_returned_while_locked = false;
  std::thread lock_thread([&]() {
    rt_handle.trySetFeedback([&](Fibonacci::Feedback &) {
      lock_held.store(true);
      const auto deadline = std::chrono::steady_clock::now() + ATTEMPTS * DELAY;
      while (!set_returned.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
      }
      set_returned_while_locked = set_returned.load();
    });
  });
  while (!lock_held.load()) {
    std::this_thread::yield();
  }
  auto result = std::make_shared<Fibonacci::Result>();
  result->sequence = {1, 1, 2, 3, 5};
  rt_handle.setSucceeded(result);
  set_returned.store(true);
  lock_thread.join();
  EXPECT_TRUE(set_returned_while_locked);

  rt_handle.runNonRealtime();
  auto wrapped_result = wait_for_result(node, client_handle, client);
  ASSERT_NE(nullptr, wrapped_result.get());
  EXPECT_EQ(wrapped_result->code, rclcpp_action::ResultCode::SUCCEEDED);
  ASSERT_NE(nullptr, wrapped_result->result.get());
  EXPECT_EQ(5, wrapped_result->result->sequence.size());
  rclcpp::shutdown();
}

// Like a controller whose update loop finishes a goal while its cancel callback cancels it
TEST(RealtimeServerGoalHandle, concurrent_results_deliver_one_matching_payload)
{
  constexpr int GOALS = 2000;
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("concurrent_results");
  const std::string server_name("~/client/concurrent_results");
  std::shared_ptr<ServerGoalHandle> server_handle;
  auto as = rclcpp_action::create_server<Fibonacci>(
    node, server_name,
    [](const rclcpp_action::GoalUUID &, std::shared_ptr<const Fibonacci::Goal>) {
      return rclcpp_action::GoalResponse::ACCEPT_AND_DEFER;
    },
    [](std::shared_ptr<ServerGoalHandle>) { return rclcpp_action::CancelResponse::ACCEPT; },
    [&](std::shared_ptr<ServerGoalHandle> accepted) { server_handle = accepted; });
  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  ASSERT_TRUE(client->wait_for_action_server(ATTEMPTS * DELAY));

  // Only the setters run on other threads, so TSan sees no races inside uninstrumented rclcpp
  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);
  auto complete = [&](const auto & future) {
    return executor.spin_until_future_complete(future, ATTEMPTS * DELAY) ==
           rclcpp::FutureReturnCode::SUCCESS;
  };

  for (int i = 0; i < GOALS; ++i) {
    server_handle.reset();
    auto goal_future = client->async_send_goal(Fibonacci::Goal());
    ASSERT_TRUE(complete(goal_future));
    auto client_handle = goal_future.get();
    ASSERT_NE(nullptr, client_handle.get());
    ASSERT_NE(nullptr, server_handle.get());
    RealtimeServerGoalHandle<Fibonacci> rt_handle(server_handle);
    rt_handle.execute();
    rt_handle.runNonRealtime();
    ASSERT_TRUE(complete(client->async_cancel_goal(client_handle)));
    ASSERT_TRUE(server_handle->is_canceling());

    auto succeeded = std::make_shared<Fibonacci::Result>();
    succeeded->sequence = {static_cast<int32_t>(rclcpp_action::ResultCode::SUCCEEDED)};
    auto canceled = std::make_shared<Fibonacci::Result>();
    canceled->sequence = {static_cast<int32_t>(rclcpp_action::ResultCode::CANCELED)};
    std::atomic<int> started = 0;
    std::atomic<bool> go = false;
    auto start_together = [&]() {
      started.fetch_add(1);
      while (!go.load()) {
      }
    };
    std::thread rt_thread([&]() {
      start_together();
      rt_handle.setSucceeded(succeeded);
    });
    std::thread cancel_thread([&]() {
      start_together();
      rt_handle.setCanceled(canceled);
    });
    while (started.load() < 2) {
    }
    go.store(true);
    rt_thread.join();
    cancel_thread.join();
    rt_handle.runNonRealtime();

    auto result_future = client->async_get_result(client_handle);
    ASSERT_TRUE(complete(result_future));
    const auto wrapped_result = result_future.get();
    ASSERT_NE(nullptr, wrapped_result.result.get());
    ASSERT_EQ(
      wrapped_result.result->sequence,
      std::vector<int32_t>{static_cast<int32_t>(wrapped_result.code)})
      << "goal " << i;
  }
  rclcpp::shutdown();
}

// Like a controller whose goal timer polls while its update loop finishes the goal
TEST(RealtimeServerGoalHandle, run_non_realtime_concurrent_with_setter)
{
  constexpr int GOALS = 10000;
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("concurrent_run_non_realtime");
  const std::string server_name("~/client/concurrent_run_non_realtime");
  std::shared_ptr<ServerGoalHandle> server_handle;
  auto as = rclcpp_action::create_server<Fibonacci>(
    node, server_name,
    [](const rclcpp_action::GoalUUID &, std::shared_ptr<const Fibonacci::Goal>) {
      return rclcpp_action::GoalResponse::ACCEPT_AND_DEFER;
    },
    [](std::shared_ptr<ServerGoalHandle>) { return rclcpp_action::CancelResponse::ACCEPT; },
    [&](std::shared_ptr<ServerGoalHandle> accepted) { server_handle = accepted; });
  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  ASSERT_TRUE(client->wait_for_action_server(ATTEMPTS * DELAY));

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);
  auto complete = [&](const auto & future) {
    return executor.spin_until_future_complete(future, ATTEMPTS * DELAY) ==
           rclcpp::FutureReturnCode::SUCCESS;
  };

  for (int i = 0; i < GOALS; ++i) {
    server_handle.reset();
    auto goal_future = client->async_send_goal(Fibonacci::Goal());
    ASSERT_TRUE(complete(goal_future));
    auto client_handle = goal_future.get();
    ASSERT_NE(nullptr, client_handle.get());
    ASSERT_NE(nullptr, server_handle.get());
    RealtimeServerGoalHandle<Fibonacci> rt_handle(server_handle);
    rt_handle.execute();
    rt_handle.runNonRealtime();

    auto result = std::make_shared<Fibonacci::Result>();
    result->sequence = {i};
    std::atomic<bool> polling = false;
    std::thread non_rt_thread([&]() {
      polling.store(true);
      while (server_handle->is_active()) {
        rt_handle.runNonRealtime();
      }
    });
    while (!polling.load()) {
    }
    rt_handle.setSucceeded(result);
    non_rt_thread.join();

    auto result_future = client->async_get_result(client_handle);
    ASSERT_TRUE(complete(result_future));
    const auto wrapped_result = result_future.get();
    EXPECT_EQ(wrapped_result.code, rclcpp_action::ResultCode::SUCCEEDED);
    ASSERT_NE(nullptr, wrapped_result.result.get());
    ASSERT_EQ(wrapped_result.result->sequence, std::vector<int32_t>{i});
  }
  rclcpp::shutdown();
}

TEST(RealtimeServerGoalHandle, send_feedback)
{
  rclcpp::init(0, nullptr);
  auto node = std::make_shared<rclcpp::Node>("send_feedback");
  ActionServerCallbacks callbacks;
  ActionClientCallbacks client_callbacks;
  const std::string server_name("~/client/send_feedback");
  auto as = make_server(node, server_name, callbacks);

  auto client = rclcpp_action::create_client<Fibonacci>(node, server_name);
  auto client_handle = send_goal(node, client, server_name, client_callbacks);
  ASSERT_NE(nullptr, client_handle.get());

  ASSERT_TRUE(callbacks.wait_for_handle(node));
  RealtimeServerGoalHandle<Fibonacci> rt_handle(callbacks.handle_);
  ASSERT_TRUE(rt_handle.valid());
  rt_handle.execute();
  rt_handle.runNonRealtime();

  std::atomic<bool> update_started = false;
  std::atomic<bool> finish_update = false;
  bool update_succeeded = false;
  std::thread update_thread([&]() {
    update_succeeded = rt_handle.trySetFeedback([&](Fibonacci::Feedback & feedback) {
      update_started.store(true);
      while (!finish_update.load()) {
        std::this_thread::yield();
      }
      feedback.sequence = {1, 1, 2, 3, 5};
    });
  });

  while (!update_started.load()) {
    std::this_thread::yield();
  }
  bool competing_update_called = false;
  EXPECT_FALSE(
    rt_handle.trySetFeedback([&](Fibonacci::Feedback &) { competing_update_called = true; }));
  EXPECT_FALSE(competing_update_called);

  finish_update.store(true);
  update_thread.join();
  EXPECT_TRUE(update_succeeded);
  rt_handle.runNonRealtime();

  EXPECT_TRUE(client_callbacks.wait_for_feedback(node));
  EXPECT_THAT(client_callbacks.feedback_sequence(), testing::ElementsAre(1, 1, 2, 3, 5));
  rclcpp::shutdown();
}
