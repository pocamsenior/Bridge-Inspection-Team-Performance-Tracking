> [!Important]  
> If you are interested in the data dictionary or any technical aspects on how the data model was made please visit the [wiki](https://github.com/pocamsenior/Bridge-Inspection-Team-Performance-Tracking/wiki).

In 2024, the structural department at **Hardesty & Hanover's Raleigh Office** won a 2 year on-going contract with North Carolina Department of Transporation (NCDOT) Structural Management Unit. Each month, the Bridge Inspection Team is assigned a fixed amount of structures to inspect and write reports for by the end of the month it was assigned for. 

> [!NOTE]  
> Previous to June 2025, teams were expected to achieve a 7-day turnaround from time of inspection to sumbission of report for each structure assigned.

# How is Data Leveraged to Support the Bridge Inspection Team?
Before inspections take place, data is collected for the structures to create a map for inspectors and to complete structure estimates for invoices to send to the DOT. Once a map is created, team leaders can plan out the month taking into account structure location and estimate inspection time based on structure type, structure square footage, structure condtion, and equipment needed for each structure.

The data collected also allows the team to gain insights into the general performance of the team in relation to the structures assigned by the DOT and help prove to the DOT that the team is reliable in completing the assignments each month.

A dashboard was created for the `Project Manager` to have a quick overview on DOT Assignments, to show `DOT` the team's progress over the contract lifetime when bidding for renewal, and to show team metrics when bidding for new opportunites. A GIS excel sheet was created to aid the `Inspection Scheduler` in creating maps and assigning trips efficiently.

<img src="https://github.com/pocamsenior/Bridge-Inspection-Team-Performance-Tracking/blob/main/300%20Reports/303%20Dashboards/dashboard-01.png">

<img src="https://github.com/pocamsenior/Bridge-Inspection-Team-Performance-Tracking/blob/main/300%20Reports/302%20Operational%20Tools/operational-tools-01.png">

# Insights
## Increase in Square Footage Assigned/Inspected
<img src="https://github.com/pocamsenior/Bridge-Inspection-Team-Performance-Tracking/blob/main/300%20Reports/304%20Insights/insight-01.png">

The team is assigned various types and sizes of structures each month that attribute the total of square footage. It is one aspect, in conjunction with others such as number of structures assigned, structure grade, and assignment type, that affects workload in both inspecting and report writing. Specifically for square footage, if a structure is larger it may take more personnel and time to inspect and write than a smaller structure. 


## Increase in Number of Structures Assigned/Inspected
<img src="https://github.com/pocamsenior/Bridge-Inspection-Team-Performance-Tracking/blob/main/300%20Reports/304%20Insights/insight-02.png">

Taking a closer look into the assignments, in 2025 the DOT assigned 8 more structures between 15,000 and 60,000 square feet and 22 more structures below 15,000 square feet. The average number of structures assigned per month also increased and the team experience an all-time high in 2026 with 37 structures assigned in a month.

<img src="https://github.com/pocamsenior/Bridge-Inspection-Team-Performance-Tracking/blob/main/300%20Reports/304%20Insights/insight-03.png">

An increase in assigned structures also means an increase of reports that need to be written and submitted from the time of inspection until the end of the month.

# Recommendations
## Hire Additional Personnel to Keep Up with Assignments
Even when omitting factors such as load ratings for Municipal assignments (see [Analyze How Municipal Workloads Affects State Workloads](#Analyze-How-Municipal-Workloads-Affect-State-Workloads) for more explanation), the nature of this project can very easily lead to burnout involving inspectors being over-worked. Not only are they out in the field during the week they are expected to write reports when returning back to the office. If a month consists of more than 20 bridges that could potentially mean an insuffcient to non-existent break before having to start on the next assignment. 

Another problem that appears is when an inspector is performing field work, the team will inevitably have a pile of unwritten reports that need to be written by the inspector once they return to the office. Although this is meant to insure they have sufficient work during their in-office week, it could easily become overwhelming if reports are either longer than average or more involved because structure conditions have declined. If the inspector is expected to return to the field next week, this leaves unwritten or half-written reports until they return as their pile grows from their next in-field week.

Hiring additional permanent personnel could alleviate workload stress overall within the team creating a healthier distribution of inspecting structures and writing reports.

# How Can This Analysis Dive Deeper Into Team Performance?
This is an on-going project that will grow as the assignment continues and as the team grows. At this moment, the main focus are the assignments the team recieves from the DOT. We are in a unique position as the project has only been active for two years and the team has collected other data that can be used to reveal more information on performance which could lead to solutions to help the team run better.

## Determining Workload Distribution Between Inspectors
Throughout the project the team has kept track of which inspection teams inspected the structures and when they inspected the structure. With this data, workload can be analyzed per team leader and assistant to gain insights on whether work is being fairly distributed (if anyone could be potentially overworked or has an unbalanced ratio of worst structures to inspect) or if the team should consider hiring another inspector to help relieve inspectors. 

## Average Report Completion Time & Team Rejection Rate
For each structure there is a report that has to be written, to keep track of reports the team has trackers for each month to make sure each structure has been written, checked, and revised before submitting to the DOT. This process is done not only to make sure each report is to standard but to also reduce the amount of rejections sent back to the team. Each phase is tracked by date and the amount of time taken to finish a report end to end based on factors such as structure square footage and structure grade could be analysed. Other analyses such as rejections per writer and checker as well as overall frequent rejections could be performed to correct common mistakes by team members.

## Analyze How Municipal Workloads Affect State Workloads
The team recieves two types of assignments, State and Municipal. State assignments are the typical assignment where teams inspect a bridge and write a report on its condition. Municipal assignments are given to a team every other year, so for example if you are given Municipal assignments throughout 2024, you will not recieve any in 2025. Municipal assignments add another layer to the workload, not only must the team inspect and write reports, they also have to perform load ratings (load capacity a structure can handle) for structures and submit by the end of the next month. So depending on the size and skillset of a team, Municipal assignments can be a two month assignment overlapping with the next month's assignment duties. Analyzing the overlap may reveal how the team can better equip for Municipal assignments.

# How Can This Project Be Improved Technically?
Using SQL as the main ETL tool would significantly reduce loading time in Power BI but also the effort taken to update information once NCDOT assigns structures.

> [!NOTE]  
> Next steps is to learn SQL and update this project as I learn to create a better experience for the data collector