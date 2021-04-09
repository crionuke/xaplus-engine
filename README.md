# xaplus-engine
Transaction processing engine based on XA and XA+ protocols.

This engine support three type of transactions:
* **Local** - boundaries of transactions are limited by one resource manager;
* **Distributed** - such transactions distributed over two or more resource managers;
* **Global** - it's main purpose of this project to implement supports of transactions distributed over several application programs.

**Links**

* The XA Specification - https://pubs.opengroup.org/onlinepubs/009680699/toc.pdf
* The XA+ Specification Version 2 - https://pubs.opengroup.org/onlinepubs/008057699/toc.pdf