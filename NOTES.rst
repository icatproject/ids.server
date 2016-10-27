IDS Locking Implementation Notes
================================

This document contains an unsorted collection of notes and
observations made while coding on the implementation of a new locking
mechanism for ids.server.


Implementation
~~~~~~~~~~~~~~

As a general note, I will concentrate on the case of storage unit
dataset first and consider the storage unit datafile case later on.


LockManager
-----------

The new LockManager class has been implementated as a Singleton to
manage the locks and to obtain the locks from.

AlreadyLockedException
----------------------

This is the exception to be raised by the lock() method if it was not
able to acquire the lock.  For the moment, the class is nested inside
the LockManager.  A better place may be found later on.  In
particular, for the `Locking in Storage Plugin Option`_, the plugin
must also be able to raise it.  Maybe move it to the package
`org.icatproject.ids.plugin` then?

This exception is an IOException.  This may be an arguable decision.


Observations Regarding the Existing Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Current Locking
---------------

The current locking mechanism is used in IdsBean.  Some operations in
the FiniteStateMachine check for locks.

Places where locking, unlocking, and checking for locks occurs:

 * class IdsBean.SO get an ARCHIVE_AND_DELETE lock as argument to the
   constructor.  The write() method finally unlocks.

 * IdsBean.checkDatafilesPresent() gets an ARCHIVE_AND_DELETE lock as
   argument and unlocks on failure.

 * IdsBean.checkOnlineAndFreeLockOnFailure() gets a lock as argument
   and unlocks on failure.

 * IdsBean.delete() acquires an ARCHIVE lock (only if storage unit
   dataset).  It calls checkOnlineAndFreeLockOnFailure().  It checks
   for a DELETE lock.

 * IdsBean.getData(preparedId, ...) acquires an ARCHIVE_AND_DELETE
   lock.  It calls checkOnlineAndFreeLockOnFailure() and
   checkDatafilesPresent().  It finally passes the lock over to
   IdsBean.SO.

 * IdsBean.getData(sessionId, ...) acquires an ARCHIVE_AND_DELETE
   lock.  It calls checkOnlineAndFreeLockOnFailure() and
   checkDatafilesPresent().  It finally passes the lock over to
   IdsBean.SO.

 * IdsBean.put() acquires an ARCHIVE lock.  It calls
   checkOnlineAndFreeLockOnFailure().

 * FiniteStateMachine.DfProcessQueue.run() check an ARCHIVE lock on
   the dataset before adding a datafile to the list of files to
   archive.

 * FiniteStateMachine.DsProcessQueue.run() check an ARCHIVE lock on
   the dataset before starting a DsArchiver on it.

 * FiniteStateMachine.getServiceStatus() compiles a json string with
   information on currently set locks.

Unsorted Observations
---------------------

 * The getData(preparedId, ...) web service call sets an
   ARCHIVE_AND_DELETE lock on the datasets in the prepared data.  But
   not on the datafiles in the prepared data or the datasets
   containing those datafiles.


Locking in Storage Plugin Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An option on top of the new locking mechanism has been discussed in
the Copenhagen meeting: add a new optional plugin call lock().  This
would allow to use file system locking to also extent the protection
also to external file access.
