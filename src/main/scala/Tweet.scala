import java.util.Date

case class Tweet(
  id: java.lang.Long,
  createdAt: Date,
  userId: java.lang.Long,
  userName: String,
  hashtag: String,
  text: String
)


