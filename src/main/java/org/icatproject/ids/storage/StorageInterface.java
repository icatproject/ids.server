/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.icatproject.ids.storage;

import java.util.HashSet;
import java.util.List;

import org.icatproject.ids.entity.DatafileEntity;

/**
 *
 * @author sn65
 */
public interface StorageInterface {
    public HashSet<String> copyDatafiles(List<DatafileEntity> datafileList);
    public String getStoragePath();
    public void clearUnusedFiles(int numberOfDays);
}
