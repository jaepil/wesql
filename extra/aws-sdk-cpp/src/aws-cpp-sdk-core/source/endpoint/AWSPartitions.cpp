/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/endpoint/AWSPartitions.h>
#include <aws/core/utils/memory/stl/AWSArray.h>

namespace Aws
{
namespace Endpoint
{
const size_t AWSPartitions::PartitionsBlobStrLen = 2525;
const size_t AWSPartitions::PartitionsBlobSize = 2526;

using PartitionsBlobT = Aws::Array<const char, AWSPartitions::PartitionsBlobSize>;
static constexpr PartitionsBlobT PartitionsBlob = {{
'{','"','p','a','r','t','i','t','i','o','n','s','"',':','[','{','"','i','d','"',':','"','a','w','s',
'"',',','"','o','u','t','p','u','t','s','"',':','{','"','d','n','s','S','u','f','f','i','x','"',':',
'"','a','m','a','z','o','n','a','w','s','.','c','o','m','"',',','"','d','u','a','l','S','t','a','c',
'k','D','n','s','S','u','f','f','i','x','"',':','"','a','p','i','.','a','w','s','"',',','"','i','m',
'p','l','i','c','i','t','G','l','o','b','a','l','R','e','g','i','o','n','"',':','"','u','s','-','e',
'a','s','t','-','1','"',',','"','n','a','m','e','"',':','"','a','w','s','"',',','"','s','u','p','p',
'o','r','t','s','D','u','a','l','S','t','a','c','k','"',':','t','r','u','e',',','"','s','u','p','p',
'o','r','t','s','F','I','P','S','"',':','t','r','u','e','}',',','"','r','e','g','i','o','n','R','e',
'g','e','x','"',':','"','^','(','u','s','|','e','u','|','a','p','|','s','a','|','c','a','|','m','e',
'|','a','f','|','i','l',')','\\','\\','-','\\','\\','w','+','\\','\\','-','\\','\\','d','+','$','"',',','"',
'r','e','g','i','o','n','s','"',':','{','"','a','f','-','s','o','u','t','h','-','1','"',':','{','}',
',','"','a','p','-','e','a','s','t','-','1','"',':','{','}',',','"','a','p','-','n','o','r','t','h',
'e','a','s','t','-','1','"',':','{','}',',','"','a','p','-','n','o','r','t','h','e','a','s','t','-',
'2','"',':','{','}',',','"','a','p','-','n','o','r','t','h','e','a','s','t','-','3','"',':','{','}',
',','"','a','p','-','s','o','u','t','h','-','1','"',':','{','}',',','"','a','p','-','s','o','u','t',
'h','-','2','"',':','{','}',',','"','a','p','-','s','o','u','t','h','e','a','s','t','-','1','"',':',
'{','}',',','"','a','p','-','s','o','u','t','h','e','a','s','t','-','2','"',':','{','}',',','"','a',
'p','-','s','o','u','t','h','e','a','s','t','-','3','"',':','{','}',',','"','a','p','-','s','o','u',
't','h','e','a','s','t','-','4','"',':','{','}',',','"','a','w','s','-','g','l','o','b','a','l','"',
':','{','}',',','"','c','a','-','c','e','n','t','r','a','l','-','1','"',':','{','}',',','"','c','a',
'-','w','e','s','t','-','1','"',':','{','}',',','"','e','u','-','c','e','n','t','r','a','l','-','1',
'"',':','{','}',',','"','e','u','-','c','e','n','t','r','a','l','-','2','"',':','{','}',',','"','e',
'u','-','n','o','r','t','h','-','1','"',':','{','}',',','"','e','u','-','s','o','u','t','h','-','1',
'"',':','{','}',',','"','e','u','-','s','o','u','t','h','-','2','"',':','{','}',',','"','e','u','-',
'w','e','s','t','-','1','"',':','{','}',',','"','e','u','-','w','e','s','t','-','2','"',':','{','}',
',','"','e','u','-','w','e','s','t','-','3','"',':','{','}',',','"','i','l','-','c','e','n','t','r',
'a','l','-','1','"',':','{','}',',','"','m','e','-','c','e','n','t','r','a','l','-','1','"',':','{',
'}',',','"','m','e','-','s','o','u','t','h','-','1','"',':','{','}',',','"','s','a','-','e','a','s',
't','-','1','"',':','{','}',',','"','u','s','-','e','a','s','t','-','1','"',':','{','}',',','"','u',
's','-','e','a','s','t','-','2','"',':','{','}',',','"','u','s','-','w','e','s','t','-','1','"',':',
'{','}',',','"','u','s','-','w','e','s','t','-','2','"',':','{','}','}','}',',','{','"','i','d','"',
':','"','a','w','s','-','c','n','"',',','"','o','u','t','p','u','t','s','"',':','{','"','d','n','s',
'S','u','f','f','i','x','"',':','"','a','m','a','z','o','n','a','w','s','.','c','o','m','.','c','n',
'"',',','"','d','u','a','l','S','t','a','c','k','D','n','s','S','u','f','f','i','x','"',':','"','a',
'p','i','.','a','m','a','z','o','n','w','e','b','s','e','r','v','i','c','e','s','.','c','o','m','.',
'c','n','"',',','"','i','m','p','l','i','c','i','t','G','l','o','b','a','l','R','e','g','i','o','n',
'"',':','"','c','n','-','n','o','r','t','h','w','e','s','t','-','1','"',',','"','n','a','m','e','"',
':','"','a','w','s','-','c','n','"',',','"','s','u','p','p','o','r','t','s','D','u','a','l','S','t',
'a','c','k','"',':','t','r','u','e',',','"','s','u','p','p','o','r','t','s','F','I','P','S','"',':',
't','r','u','e','}',',','"','r','e','g','i','o','n','R','e','g','e','x','"',':','"','^','c','n','\\',
'\\','-','\\','\\','w','+','\\','\\','-','\\','\\','d','+','$','"',',','"','r','e','g','i','o','n','s','"',
':','{','"','a','w','s','-','c','n','-','g','l','o','b','a','l','"',':','{','}',',','"','c','n','-',
'n','o','r','t','h','-','1','"',':','{','}',',','"','c','n','-','n','o','r','t','h','w','e','s','t',
'-','1','"',':','{','}','}','}',',','{','"','i','d','"',':','"','a','w','s','-','u','s','-','g','o',
'v','"',',','"','o','u','t','p','u','t','s','"',':','{','"','d','n','s','S','u','f','f','i','x','"',
':','"','a','m','a','z','o','n','a','w','s','.','c','o','m','"',',','"','d','u','a','l','S','t','a',
'c','k','D','n','s','S','u','f','f','i','x','"',':','"','a','p','i','.','a','w','s','"',',','"','i',
'm','p','l','i','c','i','t','G','l','o','b','a','l','R','e','g','i','o','n','"',':','"','u','s','-',
'g','o','v','-','w','e','s','t','-','1','"',',','"','n','a','m','e','"',':','"','a','w','s','-','u',
's','-','g','o','v','"',',','"','s','u','p','p','o','r','t','s','D','u','a','l','S','t','a','c','k',
'"',':','t','r','u','e',',','"','s','u','p','p','o','r','t','s','F','I','P','S','"',':','t','r','u',
'e','}',',','"','r','e','g','i','o','n','R','e','g','e','x','"',':','"','^','u','s','\\','\\','-','g',
'o','v','\\','\\','-','\\','\\','w','+','\\','\\','-','\\','\\','d','+','$','"',',','"','r','e','g','i','o',
'n','s','"',':','{','"','a','w','s','-','u','s','-','g','o','v','-','g','l','o','b','a','l','"',':',
'{','}',',','"','u','s','-','g','o','v','-','e','a','s','t','-','1','"',':','{','}',',','"','u','s',
'-','g','o','v','-','w','e','s','t','-','1','"',':','{','}','}','}',',','{','"','i','d','"',':','"',
'a','w','s','-','i','s','o','"',',','"','o','u','t','p','u','t','s','"',':','{','"','d','n','s','S',
'u','f','f','i','x','"',':','"','c','2','s','.','i','c','.','g','o','v','"',',','"','d','u','a','l',
'S','t','a','c','k','D','n','s','S','u','f','f','i','x','"',':','"','c','2','s','.','i','c','.','g',
'o','v','"',',','"','i','m','p','l','i','c','i','t','G','l','o','b','a','l','R','e','g','i','o','n',
'"',':','"','u','s','-','i','s','o','-','e','a','s','t','-','1','"',',','"','n','a','m','e','"',':',
'"','a','w','s','-','i','s','o','"',',','"','s','u','p','p','o','r','t','s','D','u','a','l','S','t',
'a','c','k','"',':','f','a','l','s','e',',','"','s','u','p','p','o','r','t','s','F','I','P','S','"',
':','t','r','u','e','}',',','"','r','e','g','i','o','n','R','e','g','e','x','"',':','"','^','u','s',
'\\','\\','-','i','s','o','\\','\\','-','\\','\\','w','+','\\','\\','-','\\','\\','d','+','$','"',',','"','r',
'e','g','i','o','n','s','"',':','{','"','a','w','s','-','i','s','o','-','g','l','o','b','a','l','"',
':','{','}',',','"','u','s','-','i','s','o','-','e','a','s','t','-','1','"',':','{','}',',','"','u',
's','-','i','s','o','-','w','e','s','t','-','1','"',':','{','}','}','}',',','{','"','i','d','"',':',
'"','a','w','s','-','i','s','o','-','b','"',',','"','o','u','t','p','u','t','s','"',':','{','"','d',
'n','s','S','u','f','f','i','x','"',':','"','s','c','2','s','.','s','g','o','v','.','g','o','v','"',
',','"','d','u','a','l','S','t','a','c','k','D','n','s','S','u','f','f','i','x','"',':','"','s','c',
'2','s','.','s','g','o','v','.','g','o','v','"',',','"','i','m','p','l','i','c','i','t','G','l','o',
'b','a','l','R','e','g','i','o','n','"',':','"','u','s','-','i','s','o','b','-','e','a','s','t','-',
'1','"',',','"','n','a','m','e','"',':','"','a','w','s','-','i','s','o','-','b','"',',','"','s','u',
'p','p','o','r','t','s','D','u','a','l','S','t','a','c','k','"',':','f','a','l','s','e',',','"','s',
'u','p','p','o','r','t','s','F','I','P','S','"',':','t','r','u','e','}',',','"','r','e','g','i','o',
'n','R','e','g','e','x','"',':','"','^','u','s','\\','\\','-','i','s','o','b','\\','\\','-','\\','\\','w',
'+','\\','\\','-','\\','\\','d','+','$','"',',','"','r','e','g','i','o','n','s','"',':','{','"','a','w',
's','-','i','s','o','-','b','-','g','l','o','b','a','l','"',':','{','}',',','"','u','s','-','i','s',
'o','b','-','e','a','s','t','-','1','"',':','{','}','}','}',',','{','"','i','d','"',':','"','a','w',
's','-','i','s','o','-','e','"',',','"','o','u','t','p','u','t','s','"',':','{','"','d','n','s','S',
'u','f','f','i','x','"',':','"','c','l','o','u','d','.','a','d','c','-','e','.','u','k','"',',','"',
'd','u','a','l','S','t','a','c','k','D','n','s','S','u','f','f','i','x','"',':','"','c','l','o','u',
'd','.','a','d','c','-','e','.','u','k','"',',','"','i','m','p','l','i','c','i','t','G','l','o','b',
'a','l','R','e','g','i','o','n','"',':','"','e','u','-','i','s','o','e','-','w','e','s','t','-','1',
'"',',','"','n','a','m','e','"',':','"','a','w','s','-','i','s','o','-','e','"',',','"','s','u','p',
'p','o','r','t','s','D','u','a','l','S','t','a','c','k','"',':','f','a','l','s','e',',','"','s','u',
'p','p','o','r','t','s','F','I','P','S','"',':','t','r','u','e','}',',','"','r','e','g','i','o','n',
'R','e','g','e','x','"',':','"','^','e','u','\\','\\','-','i','s','o','e','\\','\\','-','\\','\\','w','+',
'\\','\\','-','\\','\\','d','+','$','"',',','"','r','e','g','i','o','n','s','"',':','{','}','}',',','{',
'"','i','d','"',':','"','a','w','s','-','i','s','o','-','f','"',',','"','o','u','t','p','u','t','s',
'"',':','{','"','d','n','s','S','u','f','f','i','x','"',':','"','c','s','p','.','h','c','i','.','i',
'c','.','g','o','v','"',',','"','d','u','a','l','S','t','a','c','k','D','n','s','S','u','f','f','i',
'x','"',':','"','c','s','p','.','h','c','i','.','i','c','.','g','o','v','"',',','"','i','m','p','l',
'i','c','i','t','G','l','o','b','a','l','R','e','g','i','o','n','"',':','"','u','s','-','i','s','o',
'f','-','s','o','u','t','h','-','1','"',',','"','n','a','m','e','"',':','"','a','w','s','-','i','s',
'o','-','f','"',',','"','s','u','p','p','o','r','t','s','D','u','a','l','S','t','a','c','k','"',':',
'f','a','l','s','e',',','"','s','u','p','p','o','r','t','s','F','I','P','S','"',':','t','r','u','e',
'}',',','"','r','e','g','i','o','n','R','e','g','e','x','"',':','"','^','u','s','\\','\\','-','i','s',
'o','f','\\','\\','-','\\','\\','w','+','\\','\\','-','\\','\\','d','+','$','"',',','"','r','e','g','i','o',
'n','s','"',':','{','}','}',']',',','"','v','e','r','s','i','o','n','"',':','"','1','.','1','"','}',
'\0'
}};

const char* AWSPartitions::GetPartitionsBlob()
{
    return PartitionsBlob.data();
}

} // namespace Endpoint
} // namespace Aws