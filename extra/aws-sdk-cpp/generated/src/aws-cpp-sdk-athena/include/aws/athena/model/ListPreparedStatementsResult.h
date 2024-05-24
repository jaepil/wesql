﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/athena/Athena_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/athena/model/PreparedStatementSummary.h>
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
namespace Athena
{
namespace Model
{
  class ListPreparedStatementsResult
  {
  public:
    AWS_ATHENA_API ListPreparedStatementsResult();
    AWS_ATHENA_API ListPreparedStatementsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_ATHENA_API ListPreparedStatementsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline const Aws::Vector<PreparedStatementSummary>& GetPreparedStatements() const{ return m_preparedStatements; }

    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline void SetPreparedStatements(const Aws::Vector<PreparedStatementSummary>& value) { m_preparedStatements = value; }

    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline void SetPreparedStatements(Aws::Vector<PreparedStatementSummary>&& value) { m_preparedStatements = std::move(value); }

    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline ListPreparedStatementsResult& WithPreparedStatements(const Aws::Vector<PreparedStatementSummary>& value) { SetPreparedStatements(value); return *this;}

    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline ListPreparedStatementsResult& WithPreparedStatements(Aws::Vector<PreparedStatementSummary>&& value) { SetPreparedStatements(std::move(value)); return *this;}

    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline ListPreparedStatementsResult& AddPreparedStatements(const PreparedStatementSummary& value) { m_preparedStatements.push_back(value); return *this; }

    /**
     * <p>The list of prepared statements for the workgroup.</p>
     */
    inline ListPreparedStatementsResult& AddPreparedStatements(PreparedStatementSummary&& value) { m_preparedStatements.push_back(std::move(value)); return *this; }


    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline const Aws::String& GetNextToken() const{ return m_nextToken; }

    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline void SetNextToken(const Aws::String& value) { m_nextToken = value; }

    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline void SetNextToken(Aws::String&& value) { m_nextToken = std::move(value); }

    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline void SetNextToken(const char* value) { m_nextToken.assign(value); }

    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline ListPreparedStatementsResult& WithNextToken(const Aws::String& value) { SetNextToken(value); return *this;}

    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline ListPreparedStatementsResult& WithNextToken(Aws::String&& value) { SetNextToken(std::move(value)); return *this;}

    /**
     * <p>A token generated by the Athena service that specifies where to continue
     * pagination if a previous request was truncated. To obtain the next set of pages,
     * pass in the <code>NextToken</code> from the response object of the previous page
     * call.</p>
     */
    inline ListPreparedStatementsResult& WithNextToken(const char* value) { SetNextToken(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline ListPreparedStatementsResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline ListPreparedStatementsResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline ListPreparedStatementsResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    Aws::Vector<PreparedStatementSummary> m_preparedStatements;

    Aws::String m_nextToken;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace Athena
} // namespace Aws