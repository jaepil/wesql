WeSQL is a cloud-native architected MySQL that uses S3 (and S3-compatible systems) for storage,
providing cross-AZ disaster recovery with zero data loss, at nearly the cost of a single replica.

It is ideal for users who need an easy-to-deploy, scalable, cost-effective, and developer-friendly Serverless MySQL database solution, 
particularly those looking for a solution that supports BYOC (Bring Your Own Cloud). 
Whether you're a developer, DevOps engineer, or an organization.

## Why we build WeSQL

MySQL is a great and very popular database system, powering countless applications, 
with a substantial number of deployments in the cloud.

To make it easier and more cost-effective for MySQL users to manage their databases, 
cloud providers have introduced cloud-native MySQL databases. 
These offerings typically include key features such as cross-AZ disaster recovery, 
pay-as-you-go pricing and serverless architecture. 
For example, AWS Aurora is a well-known fully managed cloud-native MySQL product.

- Pay-as-you-go means you only pay for the resources you actually use, such as CPU, memory, and storage. 
You don't have to over-provision resources or pay for unused capacity 
(e.g., provisioning 100GB of storage but only using 50GB).
- Serverless allows compute resources to scale elastically based on demand. 
You don't need to provision capacity for peak workloads or pay for idle resources. 
In fact, when the database is not in use for a while — such as in development or testing environments — it can be completely shut down, 
further saving costs.

Most of these cloud-native MySQL databases adopt a compute-storage separation architecture. 
In this architecture, the database is divided into compute nodes and storage nodes, which can scale independently. 
The compute node typically runs a MySQL-based fork, while the storage node is a proprietary distributed storage system developed by the cloud provider.
Each cloud vendor's database storage system is entirely different in terms of design, implementation and API, 
which unfortunately can result in vendor lock-in.

### Our Vision
We aim to deliver a cloud-native MySQL database that can run on any cloud without vendor lock-in.
Specifically, we want the experience of deploying WeSQL in users' own VPCs (i.e. BYOC) as simple as possible.

With WeSQL, you just need to start a certain number of containers (depending on your data integrity tolerance requirements),
connect them to S3, that’s all there is to it.
Data is continuously persisted to S3, so you can safely shut down all containers and even release all EBS volumes. 
When needed, you can restart the containers and reload from S3—no data will be lost.

To achieve this simplicity and resilience, WeSQL adopts the compute-storage separation architecture, opting S3 as the storage layer,
rather than proprietary systems.
This approach follows the trends seen in modern cloud data warehouses and data lakes.

WeSQL fully replaced MySQL’s traditional disk storage with S3. All MySQL data—binlogs, schemas, storage engine metadata, WAL, and data files—are **fully** (**not partially!**) 
stored as objects in S3.
This allows WeSQL to start from an empty instance, connect to S3, load the data, and begin serving immediately with no additional setup required.

## Why use WeSQL?

### 1. MySQL Compatibility
WeSQL brings new capabilities to MySQL through an innovative architecture while using the unmodified MySQL Server codebase, 
ensuring complete MySQL compatibility. 
This allows WeSQL to quickly adopt new MySQL features and bug fixes, ensuring seamless integration with existing MySQL tools and applications.

### 2. Serverless and Pay-as-you-go
WeSQL leverages a compute-storage separation architecture, making it ideal for serverless deployment. 
By using S3 for storage instead of EBS, WeSQL dramatically reduces costs as S3 is much cheaper and billed based on usage, 
without the need for provisioned storage.

WeSQL’s compute nodes support auto-scale and auto-suspend, delivering flexibility and cost efficiency. 
Moreover, there's no need to deploy a dedicated storage service beforehand, 
making it simple to deploy WeSQL in your own VPC and gain the benefits of a serverless architecture.

### 3. Scalable Storage Capacity and Columnar Compression
Since WeSQL stores data in S3, which has no bucket size limits, you can handle extremely large datasets without the need for horizontal scaling of MySQL instances.

Additionally, WeSQL supports a compact table format that allows specified tables to be stored in a columnar format. 
This can achieve up to 10x compression compared to row-based storage, 
making it ideal for storing historical data, archived records, and other large datasets efficiently. 
This combination of scalability and compression provides both flexibility and cost savings, especially for long-term data storage.

### 4. Cross-AZ Disaster Recovery
All stateful data, including schema and metadata, is fully persisted to S3. 
Like data lakes, WeSQL stores both data and metadata in S3, avoiding the need for external databases to manage metadata.

The only exception is the recently generated binlogs (less than one second old), 
which are synchronized to Logger nodes using the Raft protocol to ensure durability in case of node failure. 
Binlogs are asynchronously flushed to S3 in batches (e.g., every few hundreds of milliseconds or when a 2MB buffer fills). 
If a data node fails, the Raft protocol elects a logger node to flush the latest binlogs to S3, ensuring S3 holds the most up-to-date data.

Because S3 is inherently cross-AZ resilient, WeSQL can achieve cross-AZ disaster recovery by deploying data and Logger nodes across three availability zones (AZs). 
In the event of an AZ failure, the only action required is to spin up a new and empty data node in another AZ, pull the data from S3, and resume operations.

### 5. Low Operational Burden
In traditional MySQL HA setups, each instance is stateful, storing data on EBS.
However, the durability of AWS EBS gp3 is 99.8-99.9%, meaning 1 in 1,000 volumes may fail annually. 
This requires time-consuming recovery from backups and manual replication reconfiguration, sometimes leading to errors like 'Fatal Error 1236' or 'ERROR 1062'.

Strictly speaking, WeSQL nodes are not entirely stateless. 
However, once the latest binlog is synced to S3 (usually within tens to hundreds of milliseconds), the nodes effectively become stateless. 
This greatly simplifies failover and migration. 
You can shut down or kill any node, launch a new one from an empty EBS volume, and it will automatically retrieve its state from S3.

Backup and recovery are simplified since WeSQL already stores all data in S3, though AWS Backup can still be used for additional redundancy if needed.

### 6. Developer-Friendly
WeSQL extends MySQL functionality with features like Online DDL, Branching, and Filters, making it more developer-friendly. 
Online DDL allows schema changes without locking tables, eliminating downtime. 
Branching enables developers to create isolated copies of databases for testing or development without affecting production, 
with commands like Prepare, Start, and Stop allowing easy database copying and synchronization. 
Filters provide fine-grained control over SQL queries by defining rules that can intercept and manipulate queries, 
applying actions like FAIL, CONTINUE, or managing concurrency. 
These features streamline development, testing, and deployment, making WeSQL more efficient and flexible for developers.

## Docs with WeSQL

For further information on WeSQL or additional documentation, visit
  <https://wesql.io/docs/introduction>

## Licensing

Portions Copyright (c) 2024, ApeCloud Inc Holding Limited. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at <http://www.apache.org/licenses/LICENSE-2.0>.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Contributors

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/demian0110"><img src="https://avatars.githubusercontent.com/u/4656979?v=4?s=100" width="100px;" alt="demian0110"/><br /><sub><b>demian0110</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=demian0110" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/zdsppl"><img src="https://avatars.githubusercontent.com/u/4177695?v=4?s=100" width="100px;" alt="dongsheng zhao"/><br /><sub><b>dongsheng zhao</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=zdsppl" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ZhaoDiankui"><img src="https://avatars.githubusercontent.com/u/16314367?v=4?s=100" width="100px;" alt="diankuizhao"/><br /><sub><b>diankuizhao</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=ZhaoDiankui" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/JingchengLi"><img src="https://avatars.githubusercontent.com/u/5918144?v=4?s=100" width="100px;" alt="lijingcheng"/><br /><sub><b>lijingcheng</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=JingchengLi" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/JashBook"><img src="https://avatars.githubusercontent.com/u/109708205?v=4?s=100" width="100px;" alt="huangzhangshu"/><br /><sub><b>huangzhangshu</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=JashBook" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/yangkaiyu-web"><img src="https://avatars.githubusercontent.com/u/59348064?v=4?s=100" width="100px;" alt="yangkaiyu-web"/><br /><sub><b>yangkaiyu-web</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=yangkaiyu-web" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/cnut"><img src="https://avatars.githubusercontent.com/u/2270906?v=4?s=100" width="100px;" alt="Peng Wang"/><br /><sub><b>Peng Wang</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=cnut" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ahjing99"><img src="https://avatars.githubusercontent.com/u/107018199?v=4?s=100" width="100px;" alt="yijing"/><br /><sub><b>yijing</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=ahjing99" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/buwen"><img src="https://avatars.githubusercontent.com/u/3852164?v=4?s=100" width="100px;" alt="buwen"/><br /><sub><b>buwen</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=buwen" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/d976045024"><img src="https://avatars.githubusercontent.com/u/62782300?v=4?s=100" width="100px;" alt="dqh"/><br /><sub><b>dqh</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=d976045024" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ksql-team"><img src="https://avatars.githubusercontent.com/u/151022862?v=4?s=100" width="100px;" alt="ksql-team"/><br /><sub><b>ksql-team</b></sub></a><br /><a href="https://github.com/wesql/wesql/commits?author=ksql-team" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->