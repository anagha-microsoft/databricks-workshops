# 01. Azure Event Hub - Lab instructions

In this lab module - we will learn to publish/consume events from Azure Event Hub with Spark Structured Streaming.  The source is the curated crimes dataset in DBFS, and the sink is DBFS in Delta format.<br>

## Unit 1. Configuring Azure Event Hub
In this unit, we will naviage to the Event Hub namespace we provisioned, create an event hub, a cosnumer group and a SAS policy.  We will capture credentials required for Spark integration.<br>

### 1.1. Provision Event Hub

![1-aeh](../../../images/2-aeh/1.png)
<br>
<hr>
<br>

![3-aeh](../../../images/2-aeh/3.png)
<br>
<hr>
<br>

![2-aeh](../../../images/2-aeh/2.png)
<br>
<hr>
<br>

### 1.2. Create consumer group within event hub

![4-aeh](../../../images/2-aeh/4.png)
<br>
<hr>
<br>

![5-aeh](../../../images/2-aeh/5.png)
<br>
<hr>
<br>


### 1.3. Create SAS policy for accessing from Spark

![11-aeh](../../../images/2-aeh/11.png)
<br>
<hr>
<br>


![12-aeh](../../../images/2-aeh/12.png)
<br>
<hr>
<br>


![13-aeh](../../../images/2-aeh/13.png)
<br>
<hr>
<br>

![14-aeh](../../../images/2-aeh/14.png)
<br>
<hr>
<br>


### 1.4. Capture connection string


