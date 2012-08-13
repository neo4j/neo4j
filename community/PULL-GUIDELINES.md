# Pull Request Guidelines

Want to add a feature to Neo4j, or fix a bug? Awesome! 
Contributing to Neo4j is easy, just fork and code. 
Once you are ready to have your changes looked at by Neo4j committers, there are a few things to do.

By following these guidelines, you significantly lessen the load on Neo4j Committers, and shorten the time it will take to handle your improvements.

### Sign the CLA

This is as quick as sending an email - http://docs.neo4j.org/chunked/milestone/cla.html

### No merges

Ensure that there are no merge-commits added by you.
If you have merged in the Neo4j master to keep up to date, undo that merge and use rebase instead.

Minimizing merges makes commit history easier to read.
Also, because we ask that each pull request is a single commit, merges are not necessary.

### Squash your changes into one commit

If you have multiple commits, you should squash them into a single one for the pull request. 
Keeping your changes in a single commit makes the commit history easier to read. 
It also makes it easy to revert and move commits around.

One way to do this is to, while standing on your local branch with your changes, create a new branch and then interactively rebase your commits into a single one.

    # On mychanges
    git branch mychanges-clean
    git checkout mychanges-clean
    
    # Assuming you have 4 commits, rebase the last four commits interactively:
    git rebase -i HEAD~4

    # In the dialog git gives you, keep your first commit, and squash all others into it.
    # Then reword the commit description to accurately depict what your commit does.
    # If applicable, include any issue numbers like so: #760

If you are asked to modify parts of your code, work in your original branch (the one with multiple commits), and follow the above process to create a fixed single commit.

### Rebase against master

Once you have a single commit, make sure it is rebased against the latest Neo4j Master.

### Run all applicable tests

Before issuing your PR, make sure that all the tests for the project you are modifying are green.


