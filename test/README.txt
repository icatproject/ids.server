Testing
=======

The IDS has a test framework for running system tests against a deployed IDS. However,
the test framework will need some manual configuration before it can be used.


Prerequisites
=============

1) IDSClient
2) A deployed ICAT
3) A deploy IDS
4) A correctly populated ICAT
5) A correctly configured test.properties
6) Run the tests

1) The test framework uses an IDS Client to make calls to the IDS. You
   can get this from:
   
   http://code.google.com/p/icat-data-service/downloads/list
   
   You will then have to add it to maven, so copy the idsclient jar to the trunk folder
   and run:  

   mvn install:install-file -Dfile=idsclient-1.0.jar -Dversion=1.0 -DgroupId=org.icatproject -Dpackaging=jar -DartifactId=idsclient

   If you still see errors, check that the idsclient version number matches the one
   listed in the IDS's pom.xml.
   
2) You will need a running ICAT that you can connect to.

3) You will need to deploy the version of the IDS that you want to test.

4) The test framework attempts to test all of the inputs the IDS may get through its
   web service interface. This requires the ICAT to be  populated in a specific way.
   
   First, there needs to be 2 user accounts. One that has read permissions for an
   investigation another that does not.
   
   Second, there needs to be one investigation that contains _one_ dataset (DS1) which in
   turn should contain _two_ datafiles (DF1 & DF2).

   Both the datafiles will need their location field set in ICAT.

5) Fill out the test.properties file with the IDs of the dataset and datafiles you have
   just created, along with the locations and MD5 sums of the two datafiles.
   
6) You can run the tests by either selecting an individual test class and running it as
   a JUnit test or executing the maven goal test.


test.properties
===============

The test framework is configured by a test.properties file. Annoyingly I can't get
java to find it in this directory so it is located at src/test/java.

