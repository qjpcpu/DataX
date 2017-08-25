package com.github.qjpcpu;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JSMR {

    private MR mr;
    private Invocable jsInvoke;


    public JSMR( String js) throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("js");
        this.mr = new MR();
        engine.put("mr", this.mr);

        engine.eval(js);

        this.jsInvoke = (Invocable) engine;
    }


    public void map(String key, Object value) throws ScriptException, NoSuchMethodException {
        this.jsInvoke.invokeFunction("map", key, value);
    }

    public void reduce() throws ScriptException, NoSuchMethodException {
        Iterator<Map.Entry<String, HashMap<String, Object>>> iter = this.mr.col.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, HashMap<String, Object>> entry = iter.next();
            String key = entry.getKey();
            this.jsInvoke.invokeFunction("reduce", key, entry.getValue());
        }

    }

    public ArrayList<Object[]> getResult() {
        return this.mr.rows;
    }

    public class MR {
        int columnCount;
        // 输出
        ArrayList<Object[]> rows;
        // 输入数据收集
        HashMap<String, HashMap<String, Object>> col;

        public MR() {
            this.rows = new ArrayList<Object[]>();
            this.col = new HashMap<String, HashMap<String, Object>>();
        }

        public void collect(String key, String property, Object value) {
            if (this.col.containsKey(key)) {
                this.col.get(key).put(property, value);
            } else {
                HashMap<String, Object> obj = new HashMap<String, Object>();
                obj.put(property, value);
                this.col.put(key, obj);
            }
        }

        public void addrow(Object[] row) throws ScriptException {
            if (row == null || (row.length != this.columnCount && this.columnCount>0)) {
                throw new ScriptException("row column count not match " + this.columnCount);
            }
            if (this.columnCount<=0){
                this.columnCount=row.length;
            }
            this.rows.add(row);
        }

        public void log(Object... message) {
            String msg ="";
            for (Object x: message){
                msg+=x.toString()+" ";
            }
            System.out.println(msg);
        }
    }

}
