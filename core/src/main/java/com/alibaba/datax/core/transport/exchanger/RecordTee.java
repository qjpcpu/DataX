package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileWriter;

/**
 * Created by jason on 28/08/2017.
 */
public class RecordTee {
    private FileWriter file = null;
    private String separator = "\t";

    public RecordTee(String filename) throws Exception {
        if (!StringUtils.isBlank(filename)) {
            new File(new File(filename).getParent()).mkdirs();
            this.file = new FileWriter(filename);
        }
    }

    public RecordTee(String filename, String separator) throws Exception {
        this(filename);
        if (!StringUtils.isBlank(separator)) {
            this.separator = separator;
        }
    }

    public void tee(Record record) {
        if (this.file == null) {
            return;
        }
        try {
            int columnSize = record.getColumnNumber();
            String suffix = this.separator;
            for (int i = 0; i < columnSize; i++) {
                if (i == columnSize - 1) {
                    suffix = "\n";
                }
                this.file.append(record.getColumn(i).asString() + suffix);
            }
            this.file.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (this.file != null) {
                this.file.flush();
                this.file.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
