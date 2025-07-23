import { Test, TestingModule } from '@nestjs/testing';
import { PostGuardsController } from './post-guards.controller';

describe('PostGuardsController', () => {
  let controller: PostGuardsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [PostGuardsController],
    }).compile();

    controller = module.get<PostGuardsController>(PostGuardsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
