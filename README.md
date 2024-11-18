# WeSQL

WeSQL is an innovative MySQL distribution that adopts a compute-storage separation architecture, with storage backed by S3 (and S3-compatible systems).
It can run on any cloud, ensuring no vendor lock-in.

WeSQL has completely replaced MySQL’s traditional disk storage with S3. All MySQL data—binlogs, schemas, storage engine metadata, WAL, and data files—are **entirely** (**not partially!**) 
stored as objects in S3.
The 11 nines of durability provided by S3 significantly enhances data reliability.
Additionally, WeSQL can start from a clean, empty instance, connect to S3, load the data, and begin serving immediately with no additional setup required.

It is ideal for users who need an easy-to-manage, cost-effective, and developer-friendly MySQL database solution, 
especially for those needing support for both Serverless and BYOC (Bring Your Own Cloud).

Feel free to start trying WeSQL in your development and testing environments!

## How to build

[Compile](https://wesql.io/docs/tutorial/binary/install)

## Docs with WeSQL

[Architecture](https://wesql.io/docs/architecture)
[Tutorial](https://wesql.io/docs/tutorial)

For further information on WeSQL or additional documentation, visit
  <https://wesql.io/docs/introduction>

## Licensing

Portions Copyright (c) 2024, ApeCloud Inc Holding Limited. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
