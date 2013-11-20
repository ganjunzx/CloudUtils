package com.paic.hbasedemo.api.core;

import java.io.IOException;

public interface RowScanner {

	Object next() throws IOException;

	void close() throws IOException;

}
