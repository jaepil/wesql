﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/quicksight/model/DeleteRoleCustomPermissionRequest.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <utility>

using namespace Aws::QuickSight::Model;
using namespace Aws::Utils::Json;
using namespace Aws::Utils;

DeleteRoleCustomPermissionRequest::DeleteRoleCustomPermissionRequest() : 
    m_role(Role::NOT_SET),
    m_roleHasBeenSet(false),
    m_awsAccountIdHasBeenSet(false),
    m_namespaceHasBeenSet(false)
{
}

Aws::String DeleteRoleCustomPermissionRequest::SerializePayload() const
{
  return {};
}



