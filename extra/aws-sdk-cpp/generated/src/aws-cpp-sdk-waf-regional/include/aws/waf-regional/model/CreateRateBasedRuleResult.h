﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/waf-regional/WAFRegional_EXPORTS.h>
#include <aws/waf-regional/model/RateBasedRule.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
namespace WAFRegional
{
namespace Model
{
  class CreateRateBasedRuleResult
  {
  public:
    AWS_WAFREGIONAL_API CreateRateBasedRuleResult();
    AWS_WAFREGIONAL_API CreateRateBasedRuleResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_WAFREGIONAL_API CreateRateBasedRuleResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>The <a>RateBasedRule</a> that is returned in the
     * <code>CreateRateBasedRule</code> response.</p>
     */
    inline const RateBasedRule& GetRule() const{ return m_rule; }

    /**
     * <p>The <a>RateBasedRule</a> that is returned in the
     * <code>CreateRateBasedRule</code> response.</p>
     */
    inline void SetRule(const RateBasedRule& value) { m_rule = value; }

    /**
     * <p>The <a>RateBasedRule</a> that is returned in the
     * <code>CreateRateBasedRule</code> response.</p>
     */
    inline void SetRule(RateBasedRule&& value) { m_rule = std::move(value); }

    /**
     * <p>The <a>RateBasedRule</a> that is returned in the
     * <code>CreateRateBasedRule</code> response.</p>
     */
    inline CreateRateBasedRuleResult& WithRule(const RateBasedRule& value) { SetRule(value); return *this;}

    /**
     * <p>The <a>RateBasedRule</a> that is returned in the
     * <code>CreateRateBasedRule</code> response.</p>
     */
    inline CreateRateBasedRuleResult& WithRule(RateBasedRule&& value) { SetRule(std::move(value)); return *this;}


    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline const Aws::String& GetChangeToken() const{ return m_changeToken; }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline void SetChangeToken(const Aws::String& value) { m_changeToken = value; }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline void SetChangeToken(Aws::String&& value) { m_changeToken = std::move(value); }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline void SetChangeToken(const char* value) { m_changeToken.assign(value); }

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline CreateRateBasedRuleResult& WithChangeToken(const Aws::String& value) { SetChangeToken(value); return *this;}

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline CreateRateBasedRuleResult& WithChangeToken(Aws::String&& value) { SetChangeToken(std::move(value)); return *this;}

    /**
     * <p>The <code>ChangeToken</code> that you used to submit the
     * <code>CreateRateBasedRule</code> request. You can also use this value to query
     * the status of the request. For more information, see
     * <a>GetChangeTokenStatus</a>.</p>
     */
    inline CreateRateBasedRuleResult& WithChangeToken(const char* value) { SetChangeToken(value); return *this;}


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline CreateRateBasedRuleResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline CreateRateBasedRuleResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline CreateRateBasedRuleResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    RateBasedRule m_rule;

    Aws::String m_changeToken;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace WAFRegional
} // namespace Aws