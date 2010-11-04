package railo.extension.io.cache.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

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
	
	private Logger log = Logger.getLogger(RiakCache.class);
	
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
			for(RiakObject ro : ros){
				Struct data = caster.toStruct(func.deserializeJSON(ro.getValue().toStringUtf8()));
				return new RiakCacheEntry(new RiakDocument(key,data)); 
			}	

		}catch(IOException e){
			String msg = "Cache key [" + key + "] could not be fetched from the server";
			log.error(msg);
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
		Object value = getCacheEntry(key.toLowerCase()).getValue();
		if(value == null){
			throw(new IOException("Key [" + key + "] has not been found."));
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
		doc.setLifeSpan(lifeSpan);
		doc.setIdleItem(idleTime);
		doc.setLastModified(now);
		doc.setExpires(now + lifeSpan);
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
			log.error("Cache key [" + doc.getKey() + "] has not been saved.");
			throw(new IOException("Riak: Cache key [" + doc.getKey() + "] has not been saved."));
		}catch(PageException e){
			e.printStackTrace();
		}
	}

}
