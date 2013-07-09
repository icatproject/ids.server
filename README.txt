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

Software
    - Subversion
    - Eclipse Standard 4.3 -> http://www.eclipse.org/downloads/
    - Maven Eclipse Plugin - Maven Integration for Eclipse WTP (Juno)

# 1. Install required software. You can install the Maven Eclipse plugin through Help->Eclipse Marketplace 
    
# 2. Check out the IDS and IDSClient projects

svn checkout http://icat-data-service.googlecode.com/svn/ids/ ids 
svn checkout http://icat-data-service.googlecode.com/svn/idsclient/ idsclient

# 3. Import both the IDS and IDSClient into Eclipse File->Import->Maven->Existing Maven Projects

# 4. Build the IDSClient. Right click the pom.xml, Run As->Maven Install

# 5. Add the IDSClient.jar to the IDS project. Right click the IDS project, Build Path->Configure Build Path, Libraries->Add External JARs

# 6. Run generate sources on the IDS project. Right click the pom.xml, Run As->Generate Sources. This builds the ICAT client API classes from the WSDLs locate in src->wsdl

# 7. Add ICAT client API classes to build path. Right click the IDS project, Build Path->Configure Build Path, Sources->Add Folder and select target->generated-sources->jaxws-wsimport

# 8. Build the IDS project. Right click the pom.xml, Run As->Maven Install. 
