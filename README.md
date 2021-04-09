# xaplus-engine
Transaction processing engine based on XA and XA+ protocols.

This engine support three type of transactions:
* **Local** - boundaries of transactions are limited by one resource manager;
* **Distributed** - such transactions distributed over two or more resource managers;
* **Global** - it's main purpose of this project to implement supports of transactions distributed over several application programs.

First, to achieve global transactions engine works with resources managers over XA protocol (see 1) to control lifecycle of applications transactions. It's two phase protocol with steps when transactions prepared and decision to commit/rollback.

Next, engine use XA+ interface (see 2) to communicate with each others of participant of global transaction.

Also superior side or initiator of global transaction make transactions log with all decision over transactions to recovery after failover.

**Links**

1. The XA Specification - https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf
1. The XA+ Specification Version 2 - https://pubs.opengroup.org/onlinepubs/008057699/toc.pdf