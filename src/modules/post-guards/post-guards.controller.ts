import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { PostGuardsService } from './post-guards.service';
import { CreatePostGuardDto } from './dto/create-post-guard.dto';
import { UpdatePostGuardDto } from './dto/update-post-guard.dto';

@Controller('post-guards')
export class PostGuardsController {
  constructor(private readonly service: PostGuardsService) {}

  @Post()
  async create(@Body() dto: CreatePostGuardDto) {
    const data = await this.service.create(dto);
    return { message: 'Post Guard created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'All Post Guard data retrieved', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return { message: 'Post Guard detail retrieved', data };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdatePostGuardDto) {
    const data = await this.service.update(+id, dto);
    return { message: 'Post Guard updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Post Guard deleted successfully', data };
  }
}
