ICAT Data Service
=================

The ICAT Data Service (IDS) is the initial implementation of the ICAT Data Service.
More information and links to the IDS specification can be found at:

http://code.google.com/p/icat-data-service/wiki/IDSMain


FOLDER STRUCTURE
================

assemble/
	Contains XML file for packaging up the IDS into its final ZIP file
	
install/
	Contains all the files that will be packaged up in a ZIP file along with the
	IDS. Includes a README for the installation and configuration setup for the IDS.
	
test/
	Contains a README explaining how to set up the testing framework.


Environment Setup
=================

No really much. If you are getting lots of errors check that you have added the
generated-sources folder to Build Path -> Sources. Also, some of the test classes
rely on the IDSClient jar. Check test/README.txt for more information.
