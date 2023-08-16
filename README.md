<div align="center">
  <h1>WeSQL Server</h1>
</div>

WeSQL server is a MySQL 8.0 branch that has added a high-compression transaction storage engine called smartengine.WeSQL server is a free, fully compatible and open source drop-in replacement for MySQL 8.0.

SmartEngine adopts the LSM-tree architecture, and its key features include:
- ***Transaction Support***:As a transactional storage engine, smartengine provides full transaction support, ensuring data consistency and reliability. It supports transaction commit, rollback, and concurrent control.
- ***Low Storage Cost***: SmartEngine effectively reduces storage space usage through technologies such as tightly packed storage formats, high compression algorithms, layered storage, and automatic space fragmentation cleanup.

<h1>Quick start</h1>

You can install the wesql server by compiling the source code, following the same method as the official MySQL 8.0.The compilation steps in an AWS EC2 environment are [here](./storage/smartengine/core/doc/compile_on_ec2.md)

The wesql server is fully compatible with the usage methods of MySQL 8.0. As a storage engine plugin for MySQL, smartengine is used in the same way as other storage engines, such as InnoDB.For example, create a smartengine table: 

```SQL
CREATE TABLE my_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
) ENGINE=SMARTENGINE;
```

<h1>Document</h1>

* [Usage Limits](./storage/smartengine/core/doc/usage_limits.md)
* [Parameter Template](./storage/smartengine/core/doc/parameter_template.md)
* [Cost-Effectiveness Comparison](./storage/smartengine/core/doc/cost_effectiveness_comparison.md)

<h1>Running Tests</h1>
There are numberous MTR cases related to smartengine.You can execute them using the following command:

```
nohup sh autotest_smartengine.sh &
```
The execution results are saved in the smartengine.mtrresult.origin and smartengine.mtrresult.retry files.


<h1>Contributing</h1>
We welcome contributions to WeSQL server! If you have any ideas, bug reports, or feature requests, please feel free to open an issue or submit a pull request.

<h1>Licensing</h1>
WeSQL server is dedicated to keeping open source open.For this project, we are using version 2 of the GNU General Public License(GPLv2).

