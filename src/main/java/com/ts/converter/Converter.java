package com.ts.converter;

import java.io.File;

public interface Converter<T extends Object> {
	
	T convert(File file);
}
