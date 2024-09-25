# Day 23

## Breaking down a real time screen into Epic, Tasks and Features

Payment screen - Epic
Break down into Tasks and Features
![alt text](<../Images/Azure DevOps/23_1.png>)

### Payment Methood - Epic

#### Check out with Amazon Pay - Feature
- Use your Amazon Account - User Story 
    - Build UI - Task 
    - Build API - Task
    - Get Authentication - Task

#### Check out with PayPal - Feature
- Using PayPal - User Story
    - Build UI - Task
    - Build API - Task
    - Get Authentication - Task

- Using PayPal Credit - User Story
    - Build UI - Task
    - Build API - Task
    - Get Authentication - Task

#### Online Credit Card or PayPal - Feature
- Using Elite Credit Card - User Story
  	- Build UI - Task
    - Build API - Task
    - Get Authentication - Task

#### Cash On Delivery(COD) - Feature 
- Create checkout - User Story 
  	- Build UI - Task
    - Build API - Task
	- Link with checkout directly - Task


## Series of Meetings in a Project

### Daily Standup Meetings
- 15 to 20 minutes	
- Beginning of the day

#### Points are discussed
- What did you work on yesterday
- What will you work on today
- Any impediments/ Blockers

### Planning Meeting 
- First day of the Sprint

**War room:**

If there is a bug then we will fix it at that time in a conference room and the members are allowed out of the meeting only when this problem is solved.

**RCA:** 
(Root Cause Analysis)
- Who did it
- How did it escape the testing
- How to avoid this scenario in future

**Work Distribution:**
- Epics and Features 
    - Tech lead, Arch and PO
- Pick the User Stories 
    - Split the User Stories to Tasks
    - Take up the tasks for yourself

- For the first few months u have buddy programmers who are accountable for your tasks. 

### Retrospective Meeting 
- Last day of the current sprint

#### Points discussed
- What went well
- What did not go well
- Any improvements
- Any ideas

## Cloning DevOps and using in Git
1. Go to `Azure DevOps`
2. Click on the `project name`
3. Then click on `Repos`
4. Then click on `Files` and copy the `clone to your computer` HTTPS link.
5. Go to vs code using code .

### Continuous Integration and Continuous Delivery Pipeline (CI/CD)
- 2006 -- Manual -- 4 Members -- TL ---- Features -- Monday -- Thursday 

- Integrate the code -- Friday -- Deploy the code to server -- QA can test it

- Big bang Approach

#### Now
- As you check in the code -- the code is taken -- Build -- and Deployed

- CI -- Continuous Integration

Checkin -- Take the code -- Build -- Test -- Create a Package/ Artifact 

Builds and gives us a package

- CD -- Continuous Delivery / Deployment
(Release)
Take the package of CI -- Deploy the package to Azure 

CI/CD is commonly used during Website Development.

**Azure Pipelines**

[Azure Pipelines Link](https://learn.microsoft.com/en-us/azure/devops/pipelines/get-started/what-is-azure-pipelines?view=azure-devops)