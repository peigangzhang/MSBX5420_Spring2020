# MSBX 5420 - Spring 2020
# Unstructured and Distributed Data Modeling and Analysis

Leeds School of Business, University of Colorado Boulder


## Contact Information

**Instructor:**  
- Dr. Peigang(Peter) Zhang (peigang.zhang@colorado.edu)

**Office hours:**  
- Thursday 7pm - 9pm, KOBL 219. You can join office hour by Zoom - https://cuboulder.zoom.us/j/2718416002


## Course Info
- Course #: MSBX 5420-003
- Topic: Unstructured and Distributed Data Modeling and Analysis
- Room: HUMN 1B90
- Days: Tuesdays
- Time: 5:30 pm to 8:00 pm
- Zoom ID: 486-957-298
    - Meeting ID: 486-957-298
    - Join via web browser: https://cuboulder.zoom.us/j/486957298
    - Join via Zoom app (using meeting ID)
    - Join via One tap mobile: +16699006833,,486957298# or +16465588656,,486957298#
    - Join via telephone: 1-669-900-6833 or 1-646-558-8656


## Schedule (subject to change)
|Date          |Topic |
|--------------|------|
|Week 1<br>January 14    |[Section 1:Course Introduction.<br>Section 2: Github, Jupyter notebook, Binder, Python and Spark Helloworld](https://docs.google.com/presentation/d/1YgUwytydpm0fcVUumcvqyZqx2rn--19eWOfEYS0SXsM/edit?usp=sharing)|
|Week 2<br>January 21    |[Section 1:Distributed File System - HDFS.<br>Section 2: HDFS Command Line, Pydoop, Linux, SSH, Run Jupyter on Cluster Server](https://docs.google.com/presentation/d/1CnB19q-2kw5-4yMbd-EFhJeQ16TKxvkSxfq3HPlE1gM/edit?usp=sharing)|
|Week 3<br>January 28    |[Section 1:Distributed Computing - Spark.<br>Section 2: Docker and PySpark Programming](https://docs.google.com/presentation/d/1cMuYXSR1_0F_PPBUbFFLuoVlYRY5NirXy0a1zwCAIDQ/edit?usp=sharing)|
|Week 4<br>February 4    |[Section 1:Distributed Computing - Spark.<br>Section 2: Git, Pycharm and PySpark Programming](https://docs.google.com/presentation/d/1oCRjm_q7rfQL_FBmxUJWhKQd4nmzTyfUtDAyh-fyOoE/edit?usp=sharing)|
|Week 5<br>February 11   |[Section 1:Spark SQL.<br>Section 2: DataFrame Programming, Setup Hdfs cluster in Google Cloud](https://docs.google.com/presentation/d/1MJ8pa66y1v9LyTlGVm_A1eKZVAKOjj63v2Uv0sfGhAA/edit?usp=sharing)|
|Week 6<br>February 18   |Kafka|
|Week 7<br>February 25   |Hive|
|Week 8<br>March 3       |Cassandra|
|Week 9<br>March 10      |Cassandra|
|Week 10<br>March 17     |Cloud computing and AWS|
|March 24                |No Class, Spring break|
|Week 11<br>March 31       |Cloud computing and AWS|
|Week 12<br>April 7       |Cloud computing and AWS|
|Week 13<br>April 14      |ElasticSearch and Kibana|
|Week 14<br>April 21      |Big data machine learning|
|Week 15<br>April 28      |Final Project presentation|


## Course Description

In this course we will study how to store, process, and extract insights from unstructured data.
For us, unstructured data is any data that cannot be directly queried using SQL.  It arises in several situations:

- The data has simply not been loaded into a relational database yet.
- The data is too big to be loaded into a traditional relational database.
- The data might not naturally make sense in the SQL paradigm (e.g. images, emails, videos).

The main problem is to figure out how to process unstructured data at large scale.
We will learn how to distribute large datasets across dozens, hundreds, or even thousands of machines.  This is “Big Data”.

In particular, we will study the following:

- How to store large datasets in the **Hdfs** filesystem (part of the **Hadoop** family of technologies).
- How to process large datasets using **Spark** (a glorified task scheduler).
- How to store streaming data in **Kafka**, a modern message queueing system.
- How to consume messages from Kafka and analyze them in near-realtime using **Spark Streaming**.
- How to query large datasets with so-called “NoSQL” datastores such as **Elasticsearch** and **Cassandra**.
- How to use cloud computing infrastructure such as **AWS** to process and analyze big data.


## Course Objectives

By the end of this course students should be able to:

- Use standard software development tools such as the Linux command line (`bash`), `git`, GitHub, and `docker`.
- Store and manipulate files in HDFS.
- Write `pyspark` scripts from within a python notebook (`jupyter`), and perform analysis to extract insights.
- Consume streaming messages from Kafka, and join/enrich streaming data using `ksql`
- Stream data into NoSQL datastores such as Elasticsearch or Cassandra, and visualize using Kibana.
- Use AWS to process and analyze big data.


## Course Materials

**Laptop:**  please bring your laptop EVERY day (including the first).  Your laptop should have minimum 4 cores and 8GB of ram (recommend 16GB or higher).

**Course website:**  everything for this course will be available on Canvas - https://canvas.colorado.edu/courses/58131,
and on Github - https://github.com/peigangzhang/MSBX5420_Spring2020


## Grading

|Component                |Pct |
|-------------------------|----|
|Homework                 |20% |
|Exam and Quiz            |20% |
|Class Presentation       |20% |
|Project                  |40% |


## Grading Scheme

|Name|Range           |
|---|-----------------|
|A  |100% to 94.0%    |
|A- |< 94.0% to 90.0% |
|B+ |< 90.0 % to 87.0%|
|B  |< 87.0 % to 83.0%|
|B- |< 83.0 % to 80.0%|
|C+ |< 80.0 % to 77.0%|
|C  |< 77.0 % to 73.0%|
|C- |< 73.0 % to 70.0%|
|D+ |< 70.0 % to 67.0%|
|D  |< 67.0 % to 63.0%|
|D- |< 63.0 % to 60.0%|
|F  |< 60.0 % to 0.0% |


## Homework

- Every student should work individually.
- Late submissions will not be accepted. 
- Homework will be submitted online via Canvas. 


### Academic Integrity 

All students of the University of Colorado at Boulder are responsible for knowing and adhering to the academic integrity policy of this institution. Violations of this policy may include: cheating, plagiarism, aid of academic dishonesty, fabrication, lying, bribery, and threatening behavior. All incidents of academic misconduct shall be reported to the Honor Code Council (honor@colorado.edu; 303-735-2273). Students who are found to be in violation of the academic integrity policy will be subject to both academic sanctions from the faculty member and non-academic sanctions (including but not limited to university probation, suspension, or expulsion). Other information on the Honor Code can be found at http://www.colorado.edu/policies/honor.html and at http://honorcode.colorado.edu. 

### Classroom Behavior 

Students and faculty each have responsibility for maintaining an appropriate learning environment. Those who fail to adhere to such behavioral standards may be subject to discipline. Professional courtesy and sensitivity are especially important with respect to individuals and topics dealing with differences of race, color, culture, religion, creed, politics, veteran’s status, sexual orientation, gender, gender identity and gender expression, age, disability, and nationalities. Class rosters are provided to the instructor with the student's legal name. I will gladly honor your request to address you by an alternate name or gender pronoun. Please advise me of this preference early in the semester so that I may make appropriate changes to my records. See policies at http://www.colorado.edu/policies/classbehavior.html and at http://www.colorado.edu/studentaffairs/judicialaffairs/code.html#student_code 

### Disability Services 

If you qualify for accommodations because of a disability, please submit to your professor a letter from Disability Services in a timely manner (for exam accommodations provide your letter at least one week prior to the exam) so that your needs can be addressed. Disability Services determines accommodations based on documented disabilities. Contact Disability Services at 303-492-8671 or by e-mail at dsinfo@colorado.edu. 
If you have a temporary medical condition or injury, see Temporary Injuries under Quick Links at Disability Services website (http://disabilityservices.colorado.edu/) and discuss your needs with your professor. 

### Policy Regarding Religious Observances 

Campus policy regarding religious observances requires that faculty make every effort to deal reasonably and fairly with all students who, because of religious obligations, have conflicts with scheduled exams, assignments or required attendance. Assignment and case report deadlines are not extended due to religious observance. If you cannot attend a class due to religious observance, please inform the instructor in advance. Please inform the instructor as soon as possible if you have an exam schedule conflict due to religious observance so alternative arrangements can be made. See full details at http://www.colorado.edu/policies/fac_relig.html. 

### Discrimination and Harassment 

The University of Colorado Boulder (CU-Boulder) is committed to maintaining a positive learning, working, and living environment. The University of Colorado does not discriminate on the basis of race, color, national origin, sex, age, disability, creed, religion, sexual orientation, or veteran status in admission and access to, and treatment and employment in, its educational programs and activities. (Regent Law, Article 10, amended 11/8/2001). CUBoulder will not tolerate acts of discrimination or harassment based upon Protected Classes or related retaliation against or by any employee or student. For purposes of this CU-Boulder policy, "Protected Classes" refers to race, color, national origin, sex, pregnancy, age, disability, creed, religion, sexual orientation, gender identity, gender expression, or veteran status. Individuals who believe they have been discriminated against should contact the Office of Discrimination and Harassment (ODH) at 303-492-2127 or the Office of Student Conduct (OSC) at 303-492-5550. Information about the ODH, the above referenced policies, and the campus resources available to assist individuals regarding discrimination or harassment can be obtained at http://hr.colorado.edu/dh/.
