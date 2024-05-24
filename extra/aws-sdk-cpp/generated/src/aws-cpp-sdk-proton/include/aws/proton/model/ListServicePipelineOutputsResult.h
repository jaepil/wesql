﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/proton/Proton_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/proton/model/Output.h>
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
namespace Proton
{
namespace Model
{
  class ListServicePipelineOutputsResult
  {
  public:
    AWS_PROTON_API ListServicePipelineOutputsResult();
    AWS_PROTON_API ListServicePipelineOutputsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);
    AWS_PROTON_API ListServicePipelineOutputsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Json::JsonValue>& result);


    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline const Aws::String& GetNextToken() const{ return m_nextToken; }

    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline void SetNextToken(const Aws::String& value) { m_nextToken = value; }

    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline void SetNextToken(Aws::String&& value) { m_nextToken = std::move(value); }

    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline void SetNextToken(const char* value) { m_nextToken.assign(value); }

    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline ListServicePipelineOutputsResult& WithNextToken(const Aws::String& value) { SetNextToken(value); return *this;}

    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline ListServicePipelineOutputsResult& WithNextToken(Aws::String&& value) { SetNextToken(std::move(value)); return *this;}

    /**
     * <p>A token that indicates the location of the next output in the array of
     * outputs, after the current requested list of outputs.</p>
     */
    inline ListServicePipelineOutputsResult& WithNextToken(const char* value) { SetNextToken(value); return *this;}


    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline const Aws::Vector<Output>& GetOutputs() const{ return m_outputs; }

    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline void SetOutputs(const Aws::Vector<Output>& value) { m_outputs = value; }

    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline void SetOutputs(Aws::Vector<Output>&& value) { m_outputs = std::move(value); }

    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline ListServicePipelineOutputsResult& WithOutputs(const Aws::Vector<Output>& value) { SetOutputs(value); return *this;}

    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline ListServicePipelineOutputsResult& WithOutputs(Aws::Vector<Output>&& value) { SetOutputs(std::move(value)); return *this;}

    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline ListServicePipelineOutputsResult& AddOutputs(const Output& value) { m_outputs.push_back(value); return *this; }

    /**
     * <p>An array of service pipeline Infrastructure as Code (IaC) outputs.</p>
     */
    inline ListServicePipelineOutputsResult& AddOutputs(Output&& value) { m_outputs.push_back(std::move(value)); return *this; }


    
    inline const Aws::String& GetRequestId() const{ return m_requestId; }

    
    inline void SetRequestId(const Aws::String& value) { m_requestId = value; }

    
    inline void SetRequestId(Aws::String&& value) { m_requestId = std::move(value); }

    
    inline void SetRequestId(const char* value) { m_requestId.assign(value); }

    
    inline ListServicePipelineOutputsResult& WithRequestId(const Aws::String& value) { SetRequestId(value); return *this;}

    
    inline ListServicePipelineOutputsResult& WithRequestId(Aws::String&& value) { SetRequestId(std::move(value)); return *this;}

    
    inline ListServicePipelineOutputsResult& WithRequestId(const char* value) { SetRequestId(value); return *this;}

  private:

    Aws::String m_nextToken;

    Aws::Vector<Output> m_outputs;

    Aws::String m_requestId;
  };

} // namespace Model
} // namespace Proton
} // namespace Aws