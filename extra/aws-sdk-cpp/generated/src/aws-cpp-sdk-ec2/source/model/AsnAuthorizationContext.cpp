﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/ec2/model/AsnAuthorizationContext.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

#include <utility>

using namespace Aws::Utils::Xml;
using namespace Aws::Utils;

namespace Aws
{
namespace EC2
{
namespace Model
{

AsnAuthorizationContext::AsnAuthorizationContext() : 
    m_messageHasBeenSet(false),
    m_signatureHasBeenSet(false)
{
}

AsnAuthorizationContext::AsnAuthorizationContext(const XmlNode& xmlNode) : 
    m_messageHasBeenSet(false),
    m_signatureHasBeenSet(false)
{
  *this = xmlNode;
}

AsnAuthorizationContext& AsnAuthorizationContext::operator =(const XmlNode& xmlNode)
{
  XmlNode resultNode = xmlNode;

  if(!resultNode.IsNull())
  {
    XmlNode messageNode = resultNode.FirstChild("Message");
    if(!messageNode.IsNull())
    {
      m_message = Aws::Utils::Xml::DecodeEscapedXmlText(messageNode.GetText());
      m_messageHasBeenSet = true;
    }
    XmlNode signatureNode = resultNode.FirstChild("Signature");
    if(!signatureNode.IsNull())
    {
      m_signature = Aws::Utils::Xml::DecodeEscapedXmlText(signatureNode.GetText());
      m_signatureHasBeenSet = true;
    }
  }

  return *this;
}

void AsnAuthorizationContext::OutputToStream(Aws::OStream& oStream, const char* location, unsigned index, const char* locationValue) const
{
  if(m_messageHasBeenSet)
  {
      oStream << location << index << locationValue << ".Message=" << StringUtils::URLEncode(m_message.c_str()) << "&";
  }

  if(m_signatureHasBeenSet)
  {
      oStream << location << index << locationValue << ".Signature=" << StringUtils::URLEncode(m_signature.c_str()) << "&";
  }

}

void AsnAuthorizationContext::OutputToStream(Aws::OStream& oStream, const char* location) const
{
  if(m_messageHasBeenSet)
  {
      oStream << location << ".Message=" << StringUtils::URLEncode(m_message.c_str()) << "&";
  }
  if(m_signatureHasBeenSet)
  {
      oStream << location << ".Signature=" << StringUtils::URLEncode(m_signature.c_str()) << "&";
  }
}

} // namespace Model
} // namespace EC2
} // namespace Aws