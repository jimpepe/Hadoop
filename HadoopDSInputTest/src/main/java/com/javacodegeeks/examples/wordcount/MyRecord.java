package com.javacodegeeks.examples.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

class MyRecord implements Writable, DBWritable {
  int Population;
  String Name, CountryCode, District;
 
  public void readFields(DataInput in) throws IOException {
    
    this.Name = Text.readString(in);
    this.CountryCode = Text.readString(in);
    this.District = Text.readString(in);
    this.Population = in.readInt();
  }
 
  public void readFields(ResultSet resultSet)
      throws SQLException {
   
    this.Name = resultSet.getString(1);
    this.CountryCode = resultSet.getString(2);
    this.District = resultSet.getString(3);
    this.Population = resultSet.getInt(4);
    
  }
 
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.Population);
    Text.writeString(out, this.Name);
  }
 
  public void write(PreparedStatement stmt) throws SQLException {
    stmt.setInt(1, this.Population);
    stmt.setString(2, this.Name);
  }
}