# kinesis-two-phase-sort
Pull from Kinesis Stream every N seconds, sort then push to new Kinesis stream.

# Dependencies and Building 

Everything runs with ORACLE JavaS.E-1.7.

The jar files in /aws-lib comprise the instalation of the aws-sdk. This can be found here:
http://aws.amazon.com/sdk-for-java/

The jar files in /apps-lib come from 3 sources, and are required to build kinesis-produces and kinesis-apps (consumers). 
Firstly, we need the amazon-kinesis-client-library (https://search.maven.org/#artifactdetails|com.amazonaws|amazon-kinesis-client|1.4.0|jar).
This is the amazon preferred way of reading from kinesis streams (handles fault-tolerances, other issues). 

Second is the amazon-kinesis-producer-library (https://search.maven.org/#artifactdetails|com.amazonaws|amazon-kinesis-producer|0.9.0|jar).
Similarly to the kinesis client library the producer library tries to handle issues like message retry etc. 

Finally, we need protobuf-java-2.6.1.jar and commons-lang-2.6.jar both these libraries are required for the code to function. 
Without these libraries the code will fail silentry WITHOUT WARNING, WITHOUT EXCEPTIONS. This is poorly/(not actually 
at all) documented. 
