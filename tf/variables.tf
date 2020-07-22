variable "project" {
  type        = string
  description = "name of google project (required)"
}

variable "buckets" {
  type = list(string)
}