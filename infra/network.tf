# A small VPC for the pipeline task: public subnets and a public IP while it runs, so it can
# reach SMARD and GitHub without a NAT gateway (about 32 USD a month). S3 goes through a free
# gateway endpoint. Nothing listens; the security group has no ingress.

resource "aws_vpc" "pipeline" {
  #checkov:skip=CKV2_AWS_11:Flow logs cost more than the pipeline; the task makes outbound HTTPS calls only
  cidr_block           = "10.42.0.0/24"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "spotgrid-pipeline" }
}

resource "aws_default_security_group" "pipeline" {
  vpc_id = aws_vpc.pipeline.id
}

resource "aws_internet_gateway" "pipeline" {
  vpc_id = aws_vpc.pipeline.id
}

resource "aws_subnet" "public" {
  #checkov:skip=CKV_AWS_130:The task needs a public IP to reach SMARD without a NAT gateway; nothing listens
  count                   = 2
  vpc_id                  = aws_vpc.pipeline.id
  cidr_block              = cidrsubnet(aws_vpc.pipeline.cidr_block, 2, count.index)
  availability_zone       = "${var.region}${["a", "b"][count.index]}"
  map_public_ip_on_launch = true

  tags = { Name = "spotgrid-pipeline-public-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.pipeline.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.pipeline.id
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.pipeline.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
}

resource "aws_security_group" "pipeline" {
  #checkov:skip=CKV2_AWS_5:Attached by the schedules in pipeline.tf, which start the task in it
  name        = "spotgrid-pipeline"
  description = "Pipeline task: outbound HTTPS only, nothing inbound"
  vpc_id      = aws_vpc.pipeline.id

  egress {
    #checkov:skip=CKV_AWS_382:SMARD, GitHub and Telegram have no fixed address ranges
    description = "HTTPS to SMARD, GitHub, Telegram and AWS APIs"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
