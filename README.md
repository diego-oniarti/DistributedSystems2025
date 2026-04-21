<h1 align="center">Course Project: Mars Distributed Storage System</h1>
<p align="center"><em>Distributed Systems 2025</em></p>

## Table of Contents

- [About the Project](#about-the-project)
- [Project Description](#project-description)
- [Repository Organization](#repository-organization)
  - [Project Assignment](#project-assignment)
  - [Java Source Code](#java-source-code)
    - [Project's Dependencies](#projects-dependencies)
  - [Report and Latex Source Code](#report-and-latex-source-code)

# About the Project
The repository contains the source code of the **Distributed Systems** project.
The project's title is *Mars Distributed Storage System*. The next
sections will describe the aim of this work and how to navigate the repository.

# Project Description
The *Mars Distributed Storage System* consists in the deployment of a distributed peer-to-peer network of data collection stations on the surface of
Mars. The system is a key-value store (database) that must ensure availability and robustness. To achieve these properties, the system is composed by several nodes that implement replication and data partitioning. In addition, the
Martian extreme environment introduce many challenges: nodes can leave, join or
crash.  
In practice, we developed the storage system exploiting the akka (Java framework) actor-based programming model. 

# Repository Organization
This section describes the repository's organization.

## Project Assignment
The file `project-description-2025.pdf` contains the project description and the implementation details.

## Java Source Code
The `app` folder contains the **Java source code** of the project.
Its **structure** is the following:
- `msg`: folder containing the classes representing the messages exchanged
during the system's operations
- `shared`: folder containing useful classes shared among clients, nodes and
coordinator
- `App.java`: class to run the project
- `AppDebug.java`: class to simulate the systems and to check its correctness
- `Client.java`: class representing the storage's clients
- `Coordinator.java`: class representing the nodes' coordinator
- `Node.java`: class representing storage's nodes

### Project's Dependencies
As we said, the code is written in **Java**. The framework used for the
simulations is **akka**. 

## Report and Latex Source Code
The `report` folder contains the **Latex source code** of the final report and the file `main.pdf`, containing the compiled **documentation**.<br>
It may be useful to first give a **contents overview** of this report:
1.  **Design Choices**: describes the implementation of the system's actors
(nodes, clients, coordinator), the operations and the simulation procedure
1.  **Assumptions and Requirements**: analyzes how we satisfied the system's
requirements and the additional assumptions we made
1.  **Testing through Simulation**: describes how we performed the tests and
the simulation