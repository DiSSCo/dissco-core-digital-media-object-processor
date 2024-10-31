/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalmediaprocessor.database.jooq.tables.records;


import eu.dissco.core.digitalmediaprocessor.database.jooq.tables.DigitalSpecimen;

import java.time.Instant;

import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class DigitalSpecimenRecord extends UpdatableRecordImpl<DigitalSpecimenRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.digital_specimen.id</code>.
     */
    public void setId(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.digital_specimen.id</code>.
     */
    public String getId() {
        return (String) get(0);
    }

    /**
     * Setter for <code>public.digital_specimen.version</code>.
     */
    public void setVersion(Integer value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.digital_specimen.version</code>.
     */
    public Integer getVersion() {
        return (Integer) get(1);
    }

    /**
     * Setter for <code>public.digital_specimen.type</code>.
     */
    public void setType(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.digital_specimen.type</code>.
     */
    public String getType() {
        return (String) get(2);
    }

    /**
     * Setter for <code>public.digital_specimen.midslevel</code>.
     */
    public void setMidslevel(Short value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.digital_specimen.midslevel</code>.
     */
    public Short getMidslevel() {
        return (Short) get(3);
    }

    /**
     * Setter for <code>public.digital_specimen.physical_specimen_id</code>.
     */
    public void setPhysicalSpecimenId(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.digital_specimen.physical_specimen_id</code>.
     */
    public String getPhysicalSpecimenId() {
        return (String) get(4);
    }

    /**
     * Setter for <code>public.digital_specimen.physical_specimen_type</code>.
     */
    public void setPhysicalSpecimenType(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.digital_specimen.physical_specimen_type</code>.
     */
    public String getPhysicalSpecimenType() {
        return (String) get(5);
    }

    /**
     * Setter for <code>public.digital_specimen.specimen_name</code>.
     */
    public void setSpecimenName(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.digital_specimen.specimen_name</code>.
     */
    public String getSpecimenName() {
        return (String) get(6);
    }

    /**
     * Setter for <code>public.digital_specimen.organization_id</code>.
     */
    public void setOrganizationId(String value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.digital_specimen.organization_id</code>.
     */
    public String getOrganizationId() {
        return (String) get(7);
    }

    /**
     * Setter for <code>public.digital_specimen.source_system_id</code>.
     */
    public void setSourceSystemId(String value) {
        set(8, value);
    }

    /**
     * Getter for <code>public.digital_specimen.source_system_id</code>.
     */
    public String getSourceSystemId() {
        return (String) get(8);
    }

    /**
     * Setter for <code>public.digital_specimen.created</code>.
     */
    public void setCreated(Instant value) {
        set(9, value);
    }

    /**
     * Getter for <code>public.digital_specimen.created</code>.
     */
    public Instant getCreated() {
        return (Instant) get(9);
    }

    /**
     * Setter for <code>public.digital_specimen.last_checked</code>.
     */
    public void setLastChecked(Instant value) {
        set(10, value);
    }

    /**
     * Getter for <code>public.digital_specimen.last_checked</code>.
     */
    public Instant getLastChecked() {
        return (Instant) get(10);
    }

    /**
     * Setter for <code>public.digital_specimen.deleted</code>.
     */
    public void setDeleted(Instant value) {
        set(11, value);
    }

    /**
     * Getter for <code>public.digital_specimen.deleted</code>.
     */
    public Instant getDeleted() {
        return (Instant) get(11);
    }

    /**
     * Setter for <code>public.digital_specimen.data</code>.
     */
    public void setData(JSONB value) {
        set(12, value);
    }

    /**
     * Getter for <code>public.digital_specimen.data</code>.
     */
    public JSONB getData() {
        return (JSONB) get(12);
    }

    /**
     * Setter for <code>public.digital_specimen.original_data</code>.
     */
    public void setOriginalData(JSONB value) {
        set(13, value);
    }

    /**
     * Getter for <code>public.digital_specimen.original_data</code>.
     */
    public JSONB getOriginalData() {
        return (JSONB) get(13);
    }

    /**
     * Setter for <code>public.digital_specimen.modified</code>.
     */
    public void setModified(Instant value) {
        set(14, value);
    }

    /**
     * Getter for <code>public.digital_specimen.modified</code>.
     */
    public Instant getModified() {
        return (Instant) get(14);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<String> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached DigitalSpecimenRecord
     */
    public DigitalSpecimenRecord() {
        super(DigitalSpecimen.DIGITAL_SPECIMEN);
    }

    /**
     * Create a detached, initialised DigitalSpecimenRecord
     */
    public DigitalSpecimenRecord(String id, Integer version, String type, Short midslevel, String physicalSpecimenId, String physicalSpecimenType, String specimenName, String organizationId, String sourceSystemId, Instant created, Instant lastChecked, Instant deleted, JSONB data, JSONB originalData, Instant modified) {
        super(DigitalSpecimen.DIGITAL_SPECIMEN);

        setId(id);
        setVersion(version);
        setType(type);
        setMidslevel(midslevel);
        setPhysicalSpecimenId(physicalSpecimenId);
        setPhysicalSpecimenType(physicalSpecimenType);
        setSpecimenName(specimenName);
        setOrganizationId(organizationId);
        setSourceSystemId(sourceSystemId);
        setCreated(created);
        setLastChecked(lastChecked);
        setDeleted(deleted);
        setData(data);
        setOriginalData(originalData);
        setModified(modified);
        resetChangedOnNotNull();
    }
}
