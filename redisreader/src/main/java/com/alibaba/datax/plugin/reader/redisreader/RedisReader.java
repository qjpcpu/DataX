package com.alibaba.datax.plugin.reader.redisreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.github.qjpcpu.JSMR;
import net.whitbeck.rdbparser.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class RedisReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originConfig = null;

        private String dumpfile = null;
        private int db = -1;
        private String keyMatch = null;
        private String js = null;


        private Pattern keyPattern;


        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            this.dumpfile = this.originConfig.getString(Key.DUMPFILE);
            if (StringUtils.isBlank(this.dumpfile)) {
                throw DataXException.asDataXException(
                        RedisReaderErrorCode.REQUIRED_VALUE,
                        "您需要指定待读取的dump.rdb文件");
            }

            this.db = this.originConfig
                    .getInt(Key.DB, -1);

            this.keyMatch = this.originConfig.getString(Key.KEY_MATCH, "*");
            this.keyMatch = this.keyMatch.replaceAll("\\*", ".+");
            this.keyPattern = Pattern.compile(this.keyMatch);
            String jsStr = this.originConfig.getString(Key.JS_STRING);
            if (StringUtils.isBlank(jsStr)) {
                String jsFile = this.originConfig.getString(Key.JS_FILE);
                if (StringUtils.isBlank(jsFile)) {
                    throw DataXException.asDataXException(
                            RedisReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定map-reduce js脚本");
                }
                try {
                    jsStr = new String(Files.readAllBytes(Paths.get(jsFile)), StandardCharsets.UTF_8);
                } catch (Exception e) {
                    LOG.error(e.toString());
                    throw DataXException.asDataXException(
                            RedisReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定map-reduce js脚本");
                }
            }
            this.js = jsStr;
        }


        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");

            LOG.info(String.format("您即将读取redis dump文件: [%s]", this.dumpfile));
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        // warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            readerSplitConfigs.add(this.originConfig.clone());
            LOG.debug("split() ok and end...");
            return readerSplitConfigs;
        }


    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration originConfig = null;

        private String dumpfile = null;
        private int db = -1;
        private String keyMatch = null;

        private Pattern keyPattern;
        private String js = null;
        private JSMR jsmr;


        @Override
        public void init() {

            this.originConfig = this.getPluginJobConf();

            this.dumpfile = this.originConfig.getString(Key.DUMPFILE);
            if (StringUtils.isBlank(this.dumpfile)) {
                throw DataXException.asDataXException(
                        RedisReaderErrorCode.REQUIRED_VALUE,
                        "您需要指定待读取的dump.rdb文件");
            }

            this.db = this.originConfig
                    .getInt(Key.DB, -1);

            this.keyMatch = this.originConfig.getString(Key.KEY_MATCH, "*");
            this.keyMatch = this.keyMatch.replaceAll("\\*", ".+");
            this.keyPattern = Pattern.compile(this.keyMatch);
            String jsStr = this.originConfig.getString(Key.JS_STRING);
            if (StringUtils.isBlank(jsStr)) {
                String jsFile = this.originConfig.getString(Key.JS_FILE);
                if (StringUtils.isBlank(jsFile)) {
                    throw DataXException.asDataXException(
                            RedisReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定map-reduce js脚本");
                }
                try {
                    jsStr = new String(Files.readAllBytes(Paths.get(jsFile)), StandardCharsets.UTF_8);
                } catch (Exception e) {
                    LOG.error(e.toString());
                    throw DataXException.asDataXException(
                            RedisReaderErrorCode.REQUIRED_VALUE,
                            "您需要指定map-reduce js脚本");
                }
            }
            this.js = jsStr;

            try {
                this.jsmr = new JSMR(this.js);
            } catch (Exception e) {
                LOG.error(e.toString());
                throw DataXException.asDataXException(
                        RedisReaderErrorCode.REQUIRED_VALUE,
                        "错误map-reduce js脚本");
            }
        }

        @Override
        public void prepare() {
            LOG.info("读取到js:\n" + this.js);
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("start read dump file...");
            try {
                parseRdbFile();

                ArrayList<Object[]> rows = jsmr.getResult();
                for (int i = 0; i < rows.size(); i++) {
                    Record record = recordSender.createRecord();
                    for (int j = 0; j < rows.get(i).length; j++) {
                        record.addColumn(new StringColumn(rows.get(i)[j].toString()));
                    }
                    recordSender.sendToWriter(record);
                }

                recordSender.flush();
            } catch (Exception e) {
                String message = String
                        .format("解析错误 : [%s]", this.dumpfile);
                LOG.error(message);
                throw DataXException.asDataXException(
                        RedisReaderErrorCode.OPEN_FILE_ERROR, message);
            }
            LOG.debug("end read dump file...");
        }

        public void parseRdbFile() throws Exception {
            try {
                RdbParser parser = new RdbParser(new File(this.dumpfile));
                Entry e;
                long db_num = 0;
                while ((e = parser.readNext()) != null) {
                    if (e.getType() == EntryType.DB_SELECT) {
                        db_num = ((DbSelect) e).getId();
                    }
                    if (this.db >= 0 && this.db != db_num) {
                        LOG.info("跳过db:" + db_num);
                        continue;
                    }
                    switch (e.getType()) {
                        case DB_SELECT:
                            LOG.debug("Processing DB: " + ((DbSelect) e).getId());
                            break;

                        case EOF:
                            LOG.debug("End of file. Checksum: ");
                            for (byte b : ((Eof) e).getChecksum()) {
                                LOG.debug(String.format("%02x", b & 0xff));
                            }
                            break;

                        case KEY_VALUE_PAIR:
                            KeyValuePair kvp = (KeyValuePair) e;
                            String key = kvp.getKeyString();
                            if (this.keyPattern.matcher(key).find()) {
                                this.jsmr.map(key, kvp.asJSON());
                            }
                            break;
                    }
                }
                jsmr.reduce();
            } catch (Exception e) {
                LOG.error(e.toString());
                throw e;
            }
        }

    }
}
