﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/qbusiness/model/CreateWebExperienceResult.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UnreferencedParam.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <utility>

using namespace Aws::QBusiness::Model;
using namespace Aws::Utils::Json;
using namespace Aws::Utils;
using namespace Aws;

CreateWebExperienceResult::CreateWebExperienceResult()
{
}

CreateWebExperienceResult::CreateWebExperienceResult(const Aws::AmazonWebServiceResult<JsonValue>& result)
{
  *this = result;
}

CreateWebExperienceResult& CreateWebExperienceResult::operator =(const Aws::AmazonWebServiceResult<JsonValue>& result)
{
  JsonView jsonValue = result.GetPayload().View();
  if(jsonValue.ValueExists("webExperienceArn"))
  {
    m_webExperienceArn = jsonValue.GetString("webExperienceArn");

  }

  if(jsonValue.ValueExists("webExperienceId"))
  {
    m_webExperienceId = jsonValue.GetString("webExperienceId");

  }


  const auto& headers = result.GetHeaderValueCollection();
  const auto& requestIdIter = headers.find("x-amzn-requestid");
  if(requestIdIter != headers.end())
  {
    m_requestId = requestIdIter->second;
  }


  return *this;
}