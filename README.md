# Hybrid Storage Box

Chinmay Manjunath
Sweekrut Suhas Joshi

1. Introduction

We present a hybrid storage box that abstracts different storage media in it and intelligently places each file into appropriate storage medium. To build a prototype, we leverage AWS cloud storage offerings and Elastic Block Storage (EBS) fits well. EBS comes in 4 flavours (two each for SSD and magnetic media): General Purpose SSD, IO Optimized SSD, Throughput optimized HDD and Cold HDD. We make use of all four of these storage offerings, each in specific proportions tailored to a given budget and performance requirements. The idea is that the users would simply see a single storage box that they can read and write from, and the data inside the box would move automatically across media based on hot and cold state, workloads, and other access patterns defined by a set of policies.

2. Related Work

Many enterprise storage companies are already involved in research and development in this area.
Examples:
Nimble Storage’s adaptive flash array
Tegile’s hybrid array 
Pure Storage’s Hybrid solution
Microsoft’s hybrid cloud
Netapp’s Data Fabric

We want to build an open source prototype that can be extended in the future to have the features an enterprise offering might have. 
