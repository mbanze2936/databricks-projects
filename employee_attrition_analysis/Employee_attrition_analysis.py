# Databricks notebook source
# MAGIC %sql
# MAGIC select * from default.employee_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### #List all the employee where attrition = Yes and group by Age

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(EmployeeCount) as Total_Employee_Count,
# MAGIC     case 
# MAGIC         when Age between 20 and 30 then '20-30'
# MAGIC         when Age between 30 and 40 then '30-40'
# MAGIC         else '40+' 
# MAGIC     end as Age_Group
# MAGIC from employee_data
# MAGIC where Attrition = "Yes"
# MAGIC GROUP BY Age
# MAGIC ORDER BY Total_Employee_Count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(EmployeeCount) as Total_Employee_Count, Department
# MAGIC FROM employee_data
# MAGIC where Attrition = "Yes"
# MAGIC GROUP BY Department

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(EmployeeCount) as Total_Employee_Count,
# MAGIC CASE 
# MAGIC   WHEN Education = 5 THEN 'Doctor'
# MAGIC   WHEN Education = 4 THEN 'Master'
# MAGIC   WHEN Education = 3 THEN 'Bachelor'
# MAGIC   WHEN Education = 2 THEN 'College'
# MAGIC   ELSE 'Below College'
# MAGIC END AS Education_Group
# MAGIC from employee_data
# MAGIC where Attrition = "Yes"
# MAGIC group by 2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(EmployeeCount) as Total_Employee_Count, EnvironmentSatisfaction
# MAGIC from employee_data
# MAGIC where Attrition = "Yes"
# MAGIC GROUP BY EnvironmentSatisfaction
# MAGIC ORDER BY Total_Employee_Count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(EmployeeCount) as Total_Employee_Count, BusinessTravel
# MAGIC from employee_data
# MAGIC where Attrition = "Yes"
# MAGIC GROUP BY BusinessTravel
# MAGIC ORDER BY Total_Employee_Count DESC;

# COMMAND ----------

# Insights from the data
1. Employees between the age 20-30
2. Employees from Research & Development dept
3. Employees that travel rarely.
4. 72 Employees that have low level of satisfaction
