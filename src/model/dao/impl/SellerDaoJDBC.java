package model.dao.impl;

import db.DB;
import db.DbException;
import java.util.List;
import model.dao.SellerDao;
import model.entites.Seller;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import model.entites.Department;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author b246131
 */
public class SellerDaoJDBC implements SellerDao {

    private Connection conn;

    public SellerDaoJDBC(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void insert(Seller obj) {
        PreparedStatement st = null;
        try{
        st = conn.prepareStatement(
                 "INSERT INTO seller "
                  + "(Name, Email, BirthDate, BaseSalary, DepartmentId) " 
                  + "VALUES "
                  + "(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
                  
        st.setString(1, obj.getName());
        st.setString(2, obj.getEmail());
        st.setDate  (3, new java.sql.Date(obj.getBirthDate().getTime()));
        st.setDouble(4, obj.getBaseSalary());
        st.setInt(5,obj.getDepartment().getId());
        
        int rowsAffected = st.executeUpdate();
        
        if(rowsAffected > 0) {
            ResultSet rs = st.getGeneratedKeys();
            if(rs.next()){
                int id = rs.getInt(1);
                obj.setId(id);
            }
            DB.closeResultSet(rs);
        } else{
            throw new DbException("Unexpected error! No rows affected!");
        }
        
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }finally{
            DB.closeStatement(st);
        }
    }

    @Override
    public void update(Seller obj) {
     PreparedStatement st = null;
        try{
        st = conn.prepareStatement(
                 "UPDATE seller "
               + "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
               + "WHERE Id = ? ");
                  
        st.setString(1, obj.getName());
        st.setString(2, obj.getEmail());
        st.setDate  (3, new java.sql.Date(obj.getBirthDate().getTime()));
        st.setDouble(4, obj.getBaseSalary());
        st.setInt(5,obj.getDepartment().getId());
        st.setInt(6, obj.getId());
        
        st.executeUpdate();
        
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        }finally{
            DB.closeStatement(st);
        }
    }

    @Override
    public void deleteById(Integer Id) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement(
                    "DELETE FROM seller "
                  + "WHERE Id = ?");
        
            st.setInt(1, Id);
            
            int rowsAffected = st.executeUpdate();
            if(rowsAffected == 0){
                throw new DbException("no seller with this id found");
            }
            
        } catch (SQLException ex) {
            throw new DbException(ex.getMessage());
        }
    }

    @Override
    public Seller findById(Integer Id) {
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            st = conn.prepareStatement(
                    "SELECT seller.*,department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "WHERE seller.Id = ?");

            st.setInt(1, Id);
            rs = st.executeQuery();

            if (rs.next()) {
                Department dept = instancianteDepartment(rs);

                Seller obj = instancianteSeller(rs, dept);
                return obj;
            }
            return null;
        } catch (SQLException e) {
            throw new db.DbIntegrityException("Error findBy Seller: " + e);
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }

    }

    @Override
    public List<Seller> findAll() {
      
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            st = conn.prepareStatement(
                    "SELECT seller.*,department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "ORDER BY Name");
            
            
            rs = st.executeQuery();

            List<Seller> list = new ArrayList<>();
            Map<Integer, Department> map = new HashMap<>();

            while (rs.next()) {
                Department dept = map.get(rs.getInt("DepartmentId"));
                
                if (dept == null) {
                    dept = instancianteDepartment(rs);
                    map.put(rs.getInt("DepartmentId"), dept);

                }
                Seller obj = instancianteSeller(rs, dept);
                list.add(obj);
            }
            return list;
        } catch (SQLException e) {
            throw new db.DbIntegrityException("Error findBy Seller: " + e);
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        } 
    }

    private Department instancianteDepartment(ResultSet rs) throws SQLException {
        Department dept = new Department();
        dept.setId(rs.getInt("DepartmentId"));
        dept.setName(rs.getString("DepName"));
        return dept;

    }

    private Seller instancianteSeller(ResultSet rs, Department dept) throws SQLException {
        Seller obj = new Seller();
        obj.setId(rs.getInt("Id"));
        obj.setName(rs.getString("Name"));
        obj.setEmail(rs.getString("Email"));
        obj.setBaseSalary(rs.getDouble("BaseSalary"));
        obj.setBirthDate(rs.getDate("BirthDate"));
        obj.setDepartment(dept);
        return obj;
    }

    @Override
    public List<Seller> findByDepartment(Department department) {

        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            st = conn.prepareStatement(
                    "SELECT seller.*,department.Name as DepName "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.DepartmentId = department.Id "
                    + "WHERE DepartmentId = ? "
                    + "ORDER BY Name");

            st.setInt(1, department.getId());
            rs = st.executeQuery();

            List<Seller> list = new ArrayList<>();
            Map<Integer, Department> map = new HashMap<>();

            while (rs.next()) {
                Department dept = map.get(rs.getInt("DepartmentId"));
                
                if (dept == null) {
                    dept = instancianteDepartment(rs);
                    map.put(rs.getInt("DepartmentId"), dept);

                }
                Seller obj = instancianteSeller(rs, dept);
                list.add(obj);
            }
            return list;
        } catch (SQLException e) {
            throw new db.DbIntegrityException("Error findBy Seller: " + e);
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }

    }

}
