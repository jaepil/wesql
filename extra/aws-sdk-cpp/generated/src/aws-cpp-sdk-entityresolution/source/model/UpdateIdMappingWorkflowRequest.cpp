﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/entityresolution/model/UpdateIdMappingWorkflowRequest.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <utility>

using namespace Aws::EntityResolution::Model;
using namespace Aws::Utils::Json;
using namespace Aws::Utils;

UpdateIdMappingWorkflowRequest::UpdateIdMappingWorkflowRequest() : 
    m_descriptionHasBeenSet(false),
    m_idMappingTechniquesHasBeenSet(false),
    m_inputSourceConfigHasBeenSet(false),
    m_outputSourceConfigHasBeenSet(false),
    m_roleArnHasBeenSet(false),
    m_workflowNameHasBeenSet(false)
{
}

Aws::String UpdateIdMappingWorkflowRequest::SerializePayload() const
{
  JsonValue payload;

  if(m_descriptionHasBeenSet)
  {
   payload.WithString("description", m_description);

  }

  if(m_idMappingTechniquesHasBeenSet)
  {
   payload.WithObject("idMappingTechniques", m_idMappingTechniques.Jsonize());

  }

  if(m_inputSourceConfigHasBeenSet)
  {
   Aws::Utils::Array<JsonValue> inputSourceConfigJsonList(m_inputSourceConfig.size());
   for(unsigned inputSourceConfigIndex = 0; inputSourceConfigIndex < inputSourceConfigJsonList.GetLength(); ++inputSourceConfigIndex)
   {
     inputSourceConfigJsonList[inputSourceConfigIndex].AsObject(m_inputSourceConfig[inputSourceConfigIndex].Jsonize());
   }
   payload.WithArray("inputSourceConfig", std::move(inputSourceConfigJsonList));

  }

  if(m_outputSourceConfigHasBeenSet)
  {
   Aws::Utils::Array<JsonValue> outputSourceConfigJsonList(m_outputSourceConfig.size());
   for(unsigned outputSourceConfigIndex = 0; outputSourceConfigIndex < outputSourceConfigJsonList.GetLength(); ++outputSourceConfigIndex)
   {
     outputSourceConfigJsonList[outputSourceConfigIndex].AsObject(m_outputSourceConfig[outputSourceConfigIndex].Jsonize());
   }
   payload.WithArray("outputSourceConfig", std::move(outputSourceConfigJsonList));

  }

  if(m_roleArnHasBeenSet)
  {
   payload.WithString("roleArn", m_roleArn);

  }

  return payload.View().WriteReadable();
}



