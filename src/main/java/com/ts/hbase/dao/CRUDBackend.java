package com.ts.hbase.dao;

import java.util.List;

import com.ts.exception.BackendException;

public interface CRUDBackend<T extends Object> {

	T update(T entity) throws BackendException;

	void create(T entity) throws BackendException;

	void remove(T entity) throws BackendException;

	T read(String identifier) throws BackendException;

	List<T> list() throws BackendException;
}