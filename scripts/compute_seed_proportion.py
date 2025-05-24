take all_seed_users as input
for each user, calculate and save the proportion of comments in seed subreddits vs all subreddits in --out directory (also save the number of total comments by that user, bc otherwise users with few comments may skew the results)


for each node in the overlap graph, there should be information which "radicalization percentiles" this subreddit is part of

somehow in the middle, before building the graph, there is a gap in the data - how do I get from the user-comments-proportion information to getting the necessary info for each node?