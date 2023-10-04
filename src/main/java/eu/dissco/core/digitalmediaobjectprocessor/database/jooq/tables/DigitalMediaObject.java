/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalmediaobjectprocessor.database.jooq.tables;


import eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Indexes;
import eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Keys;
import eu.dissco.core.digitalmediaobjectprocessor.database.jooq.Public;
import eu.dissco.core.digitalmediaobjectprocessor.database.jooq.tables.records.DigitalMediaObjectRecord;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function10;
import org.jooq.Index;
import org.jooq.JSONB;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row10;
import org.jooq.Schema;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class DigitalMediaObject extends TableImpl<DigitalMediaObjectRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.digital_media_object</code>
     */
    public static final DigitalMediaObject DIGITAL_MEDIA_OBJECT = new DigitalMediaObject();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<DigitalMediaObjectRecord> getRecordType() {
        return DigitalMediaObjectRecord.class;
    }

    /**
     * The column <code>public.digital_media_object.id</code>.
     */
    public final TableField<DigitalMediaObjectRecord, String> ID = createField(DSL.name("id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.version</code>.
     */
    public final TableField<DigitalMediaObjectRecord, Integer> VERSION = createField(DSL.name("version"), SQLDataType.INTEGER.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.type</code>.
     */
    public final TableField<DigitalMediaObjectRecord, String> TYPE = createField(DSL.name("type"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.digital_media_object.digital_specimen_id</code>.
     */
    public final TableField<DigitalMediaObjectRecord, String> DIGITAL_SPECIMEN_ID = createField(DSL.name("digital_specimen_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.media_url</code>.
     */
    public final TableField<DigitalMediaObjectRecord, String> MEDIA_URL = createField(DSL.name("media_url"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.created</code>.
     */
    public final TableField<DigitalMediaObjectRecord, Instant> CREATED = createField(DSL.name("created"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.last_checked</code>.
     */
    public final TableField<DigitalMediaObjectRecord, Instant> LAST_CHECKED = createField(DSL.name("last_checked"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.deleted</code>.
     */
    public final TableField<DigitalMediaObjectRecord, Instant> DELETED = createField(DSL.name("deleted"), SQLDataType.INSTANT, this, "");

    /**
     * The column <code>public.digital_media_object.data</code>.
     */
    public final TableField<DigitalMediaObjectRecord, JSONB> DATA = createField(DSL.name("data"), SQLDataType.JSONB.nullable(false), this, "");

    /**
     * The column <code>public.digital_media_object.original_data</code>.
     */
    public final TableField<DigitalMediaObjectRecord, JSONB> ORIGINAL_DATA = createField(DSL.name("original_data"), SQLDataType.JSONB.nullable(false), this, "");

    private DigitalMediaObject(Name alias, Table<DigitalMediaObjectRecord> aliased) {
        this(alias, aliased, null);
    }

    private DigitalMediaObject(Name alias, Table<DigitalMediaObjectRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.digital_media_object</code> table
     * reference
     */
    public DigitalMediaObject(String alias) {
        this(DSL.name(alias), DIGITAL_MEDIA_OBJECT);
    }

    /**
     * Create an aliased <code>public.digital_media_object</code> table
     * reference
     */
    public DigitalMediaObject(Name alias) {
        this(alias, DIGITAL_MEDIA_OBJECT);
    }

    /**
     * Create a <code>public.digital_media_object</code> table reference
     */
    public DigitalMediaObject() {
        this(DSL.name("digital_media_object"), null);
    }

    public <O extends Record> DigitalMediaObject(Table<O> child, ForeignKey<O, DigitalMediaObjectRecord> key) {
        super(child, key, DIGITAL_MEDIA_OBJECT);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.asList(Indexes.DIGITAL_MEDIA_OBJECT_DIGITAL_SPECIMEN_ID_URL, Indexes.DIGITAL_MEDIA_OBJECT_ID_IDX, Indexes.DIGITAL_MEDIA_OBJECT_ID_VERSION_URL);
    }

    @Override
    public UniqueKey<DigitalMediaObjectRecord> getPrimaryKey() {
        return Keys.DIGITAL_MEDIA_OBJECT_PK;
    }

    @Override
    public DigitalMediaObject as(String alias) {
        return new DigitalMediaObject(DSL.name(alias), this);
    }

    @Override
    public DigitalMediaObject as(Name alias) {
        return new DigitalMediaObject(alias, this);
    }

    @Override
    public DigitalMediaObject as(Table<?> alias) {
        return new DigitalMediaObject(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public DigitalMediaObject rename(String name) {
        return new DigitalMediaObject(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public DigitalMediaObject rename(Name name) {
        return new DigitalMediaObject(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public DigitalMediaObject rename(Table<?> name) {
        return new DigitalMediaObject(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row10 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row10<String, Integer, String, String, String, Instant, Instant, Instant, JSONB, JSONB> fieldsRow() {
        return (Row10) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function10<? super String, ? super Integer, ? super String, ? super String, ? super String, ? super Instant, ? super Instant, ? super Instant, ? super JSONB, ? super JSONB, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function10<? super String, ? super Integer, ? super String, ? super String, ? super String, ? super Instant, ? super Instant, ? super Instant, ? super JSONB, ? super JSONB, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}