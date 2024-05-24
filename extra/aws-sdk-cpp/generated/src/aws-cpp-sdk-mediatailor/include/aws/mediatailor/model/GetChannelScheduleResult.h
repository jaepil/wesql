﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/mediatailor/MediaTailor_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/mediatailor/model/ScheduleEntry.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Json
{
  class JsonValue;
} // namespace Json
} // namespace Utils
namespace MediaTailor
{
namespace Model
{
  class GetChannelScheduleResult
  {
  public:
    AWS_MEDIATAILOR_API GetChannelScheduleResult();
    AWS_MEDIATAILOR_API GetChannelScheduleResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_MEDIATAILOR_API GetChannelScheduleResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline const Aws::Vector<ScheduleEntry>& GetItems() const{ return m_items; }

    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline void SetItems(const Aws::Vector<ScheduleEntry>& value) { m_items = value; }

    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline void SetItems(Aws::Vector<ScheduleEntry>&& value) { m_items = std::move(value); }

    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline GetChannelScheduleResult& WithItems(const Aws::Vector<ScheduleEntry>& value) { SetItems(value); return *this;}

    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline GetChannelScheduleResult& WithItems(Aws::Vector<ScheduleEntry>&& value) { SetItems(std::move(value)); return *this;}

    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline GetChannelScheduleResult& AddItems(const ScheduleEntry& value) { m_items.push_back(value); return *this; }

    /**
     * <p>A list of schedule entries for the channel.</p>
     */
    inline GetChannelScheduleResult& AddItems(ScheduleEntry&& value) { m_items.push_back(std::move(value)); return *this; }


    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline const Aws::String& GetNextToken() const{ return m_nextToken; }

    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline void SetNextToken(const Aws::String& value) { m_nextToken = value; }

    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline void SetNextToken(Aws::String&& value) { m_nextToken = std::move(value); }

    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline void SetNextToken(const char* value) { m_nextToken.assign(value); }

    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline GetChannelScheduleResult& WithNextToken(const Aws::String& value) { SetNextToken(value); return *this;}

    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline GetChannelScheduleResult& WithNextToken(Aws::String&& value) { SetNextToken(std::move(value)); return *this;}

    /**
     * <p>Pagination token returned by the list request when results exceed the maximum
     * allowed. Use the token to fetch the next page of results.</p>
     */
    inline GetChannelScheduleResult& WithNextToken(const char* value) { SetNextToken(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline GetChannelScheduleResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline GetChannelScheduleResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline GetChannelScheduleResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    Aws::Vector<ScheduleEntry> m_items;

    Aws::String m_nextToken;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace MediaTailor
} // namespace Aws