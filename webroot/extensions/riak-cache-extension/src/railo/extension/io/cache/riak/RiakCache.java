package railo.extension.io.cache.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.protobuf.ByteString;
import com.trifork.riak.KeySource;
import com.trifork.riak.RiakClient;
import com.trifork.riak.RiakObject;

import railo.commons.io.cache.Cache;
import railo.commons.io.cache.CacheEntry;
import railo.commons.io.cache.CacheEntryFilter;
import railo.commons.io.cache.CacheKeyFilter;
import railo.extension.io.util.Functions;
import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.config.Config;
import railo.runtime.exp.PageException;
import railo.runtime.type.Struct;
import railo.runtime.util.Cast;

public class RiakCache implements Cache {
	
	private String cacheName;
	private String host;
	private int port;
	private RiakClient rc;
	private String bucket;
	private Functions func = new Functions();
	
	//counters
	private int hits = 0;
	private int misses = 0;
	
	static Logger logger = Logger.getLogger(RiakCache.class);
	
	@Override
	public void init(String cacheName, Struct args) throws IOException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		
		this.cacheName = cacheName;
		
		try{
			
			this.host = caster.toString(args.get("host"));
			this.port = caster.toInteger(args.get("port"));
			this.bucket = caster.toString(args.get("bucket")); 
				
		}catch (PageException e) {
			e.printStackTrace();
		}
		
		
		this.rc = new RiakClient(this.host,this.port);
		this.rc.setClientID(this.bucket);
		
		Properties props = new Properties();
		props.setProperty("log4j.rootLogger","INFO,A1");
		props.setProperty("log4j.appender.A1","org.apache.log4j.ConsoleAppender");
		props.setProperty("log4j.appender.A1.layout","org.apache.log4j.PatternLayout");
		props.setProperty("log4j.appender.A1.layout.ConversionPattern","%-5p %d %C{1} - %m%n");				
		PropertyConfigurator.configure(props);
		
		logger.info("Cache id [" + this.cacheName + "] initialized.");
		
	}

	public void init(Config config ,String[] cacheName,Struct[] arguments){
		//Not used at the moment
	}
	
	public void init(Config config, String cacheName, Struct arguments) {
		try {
		init(cacheName,arguments);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean contains(String key) {
		try{
			RiakObject[] ro = this.rc.fetch(this.bucket, key.toLowerCase());
			return true;
		}catch(IOException e){
			return false;
		}
	}

	@Override
	public List entries() {		
		ArrayList<CacheEntry> result = new ArrayList<CacheEntry>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		result.add(getCacheEntry(keys.next().toStringUtf8()));
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public List entries(CacheKeyFilter filter) {
		ArrayList<CacheEntry> result = new ArrayList<CacheEntry>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		if(filter.accept(key)){
			 		result.add(getCacheEntry(key));		 			
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public List entries(CacheEntryFilter filter) {
		ArrayList<CacheEntry> result = new ArrayList<CacheEntry>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		CacheEntry entry = getCacheEntry(key);
		 		if(filter.accept(entry)){
			 		result.add(entry);		 			
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public CacheEntry getCacheEntry(String key) throws IOException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		Functions func = new Functions();

		try{

			RiakObject[] ros = this.rc.fetch(this.bucket, key.toLowerCase());
			
			if(ros.length == 0){
				String msg = "Cache key [" + key + "] could not be fetched from the server";
				logger.error(msg);
				throw(new IOException(msg));				
			}
			
			for(RiakObject ro : ros){
				Struct data = caster.toStruct(func.deserializeJSON(ro.getValue().toStringUtf8()));
				RiakDocument doc = new RiakDocument(key,data);
				//update the last hit
				doc.setLastHit(caster.toLongValue(System.currentTimeMillis()));
				this.saveDocument(doc);
				//return the entry
				return new RiakCacheEntry(doc); 
			}	

		}catch(IOException e){
			String msg = "Cache key [" + key + "] could not be fetched from the server";
			logger.error(msg);
			throw(new IOException(msg));
		}catch(PageException e){
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public CacheEntry getCacheEntry(String key, CacheEntry defaultValue) {
		try{
			return getCacheEntry(key);
		}catch(IOException e){
			return defaultValue;
		}		
	}

	@Override
	public Struct getCustomInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(String key) throws IOException {
		Object value;
		try{
			value = getCacheEntry(key.toLowerCase()).getValue();			
		}
		catch(IOException e){
			return null;
		}
		return value;
	}

	@Override
	public Object getValue(String key, Object defaultValue) {
		try{
			this.hits++;
			return getValue(key.toLowerCase());			
		}catch (IOException e) {
			this.misses++;
			return defaultValue;
		}
	}

	@Override
	public long hitCount() {
		return this.hits;
	}

	@Override
	public List keys() {
		ArrayList<String> result = new ArrayList<String>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		result.add(keys.next().toStringUtf8());
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public List keys(CacheKeyFilter filter) {
		ArrayList<String> result = new ArrayList<String>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		if(filter.accept(key)){
			 		result.add(key);		 			
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public List keys(CacheEntryFilter filter) {
		ArrayList<String> result = new ArrayList<String>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		CacheEntry entry = getCacheEntry(key);
		 		if(filter.accept(entry)){
			 		result.add(key);		 			
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public long missCount() {
		return this.misses;
	}

	@Override
	public void put(String key, Object value, Long idleTime, Long lifeSpan) {
		Functions func = new Functions();
		String val = "";
		
		long now = System.currentTimeMillis();
		long idle = idleTime==null?0:idleTime.longValue();
		long life = lifeSpan==null?0:lifeSpan.longValue();
		
		try{
			val = func.serialize(value); 
		}catch(PageException e){
			e.printStackTrace();
		}
				
		RiakDocument doc = new RiakDocument(key.toLowerCase());
		doc.setCreated(now);
		doc.setLifeSpan(life);
		doc.setIdleItem(idle);
		doc.setLastHit(now);
		doc.setExpires(now + life);
		doc.setValue(val);
		try{
			this.saveDocument(doc);
		}catch(IOException e){
			e.printStackTrace();
		}
				
	}

	@Override
	public boolean remove(String key) {		
		try{
			this.rc.delete(this.bucket, key);
		}catch(IOException e){
			return false;
		}
		return true;
	}

	@Override
	public int remove(CacheKeyFilter filter) {
		int counter =0;
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		if(filter.accept(key)){
		 			Boolean res = remove(key);
		 			if(res){
		 				counter++;
		 			}
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}

		return counter;
	}

	@Override
	public int remove(CacheEntryFilter filter) {
		int counter =0;
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		CacheEntry entry = getCacheEntry(key);
		 		if(filter.accept(entry)){
		 			Boolean res = remove(key);
		 			if(res){
		 				counter++;
		 			}
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}

		return counter;
	}

	@Override
	public List values() {
		ArrayList<Object> result = new ArrayList<Object>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		CacheEntry entry = getCacheEntry(key);
		 		result.add(entry.getValue());
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public List values(CacheKeyFilter filter) {
		ArrayList<Object> result = new ArrayList<Object>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		if(filter.accept(key)){
			 		CacheEntry entry = getCacheEntry(key);
			 		result.add(entry.getValue());		 			
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public List values(CacheEntryFilter filter) {
		ArrayList<Object> result = new ArrayList<Object>();
		ByteString bucket = ByteString.copyFromUtf8(this.bucket);
		
		try{
		 	KeySource keys = this.rc.listKeys(bucket);			
		 	while(keys.hasNext()){
		 		String key = keys.next().toStringUtf8();
		 		CacheEntry entry = getCacheEntry(key);
		 		if(filter.accept(entry)){
			 		result.add(entry.getValue());		 			
		 		}
		 	}		 	
		}catch(IOException e){
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * Flush the invalid docs for expires and timeidle
	 */
	public void flushInvalid() {
		
	}
	
	/**
	 * Private method to persist a RiakDocument instance
	 * @param doc
	 */
	private void saveDocument(RiakDocument doc) throws IOException{
		Functions func = new Functions();
		Struct data = doc.getData();
		
		try{
			
			String json = func.serializeJSON(data,false);
			RiakObject ro = new RiakObject(this.bucket, doc.getKey(),json);
			this.rc.store(ro);				
			
		}catch(IOException e){
			logger.error("Cache key [" + doc.getKey() + "] has not been saved.");
			throw(new IOException("Riak: Cache key [" + doc.getKey() + "] has not been saved."));
		}catch(PageException e){
			e.printStackTrace();
		}
	}

	
}
