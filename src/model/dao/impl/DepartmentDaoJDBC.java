/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package model.dao.impl;

import db.DB;
import db.DbException;
import java.sql.Connection;
import java.util.List;
import model.dao.DepartmentDao;
import model.entites.Department;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author b246131
 */
public class DepartmentDaoJDBC implements DepartmentDao {

    public Connection conn;

    public DepartmentDaoJDBC(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void insert(Department obj) {

        PreparedStatement st = null;

        try {
            st = conn.prepareStatement("INSERT INTO department "
                    + "(Name) "
                    + "VALUES "
                    + "(?)", Statement.RETURN_GENERATED_KEYS);
            
            st.setString(1, obj.getName());

            int rowsAffected = st.executeUpdate();

            if (rowsAffected > 0) {
                ResultSet rs = st.getGeneratedKeys();
                if (rs.next()) {
                    int id = rs.getInt(1);
                    obj.setId(id);

                }
                DB.closeResultSet(rs);

            }

        } 
        catch (SQLException ex) {
            throw new DbException("Error: " + ex.getMessage());
        }
        finally{
            DB.closeStatement(st);
        }

    }

    @Override
    public void update(Department obj) {
        PreparedStatement st = null;
        
        try {
            st = conn.prepareStatement("UPDATE department "
               + "SET Name = ?"
               + "WHERE Id = ? ");
            
            st.setString(1, obj.getName());
            st.setInt(2, obj.getId());
            
            st.executeUpdate();
            
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }
    }

    @Override
    public void deleteById(Integer Id) {
        PreparedStatement st = null;
        try {
            st= conn.prepareStatement(
                    "DELETE FROM department "
                  + "WHERE Id = ?");
                  
            st.setInt(1, Id);
            
            int rowsAffected = st.executeUpdate();
            if(rowsAffected == 0){
                throw new DbException("no department with this id found");
            }
                  
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }
    }

    @Override
    public Department findById(Integer Id) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            st = conn.prepareStatement(
                    "SELECT * FROM department WHERE Id = ?");
            
            st.setInt(1, Id);
            rs = st.executeQuery();
            if(rs.next()){
            
                Department obj = new Department();
                obj.setId(rs.getInt("Id"));
                obj.setName(rs.getString("Name"));
                return obj;
            
            }
            return null;
            
        } catch (SQLException e) {
            throw new DbException("Error findBy Department: " + e);
        }finally{
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    @Override
    public List<Department> findAll() {
          PreparedStatement st = null;
          ResultSet rs = null;
        try {
            st = conn.prepareStatement(
                    "SELECT * FROM department");
                    
            List<Department> list = new ArrayList<>();
            Map<Integer, String> map = new HashMap<>();
            rs = st.executeQuery();
            while(rs.next()){
            
                Department obj = new Department();
                obj.setId(rs.getInt("Id"));
                obj.setName(rs.getString("Name"));
                
                list.add(obj);
            
            }
            return list;
            
        } catch (SQLException e) {
            throw new DbException("Error findBy Department: " + e);
        }finally{
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

}
